#[allow(
    clippy::single_match_else,
    clippy::collapsible_if,
    clippy::needless_borrows_for_generic_args,
    clippy::match_same_arms
)]
mod type_mapping;

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Data, DeriveInput, Fields};

use type_mapping::FieldKind;

struct FieldInfo {
    name: syn::Ident,
    name_str: String,
    kind: FieldKind,
    col_idx: usize,
}

pub(crate) fn expand(input: DeriveInput) -> Result<TokenStream, syn::Error> {
    let struct_name = &input.ident;
    let vis = &input.vis;

    let fields = match &input.data {
        Data::Struct(ds) => match &ds.fields {
            Fields::Named(named) => &named.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    &input,
                    "RheiSchema can only be derived for structs with named fields",
                ));
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                &input,
                "RheiSchema can only be derived for structs",
            ));
        }
    };

    let field_infos: Vec<FieldInfo> = fields
        .iter()
        .enumerate()
        .map(|(idx, f)| {
            let name = f
                .ident
                .clone()
                .ok_or_else(|| syn::Error::new_spanned(f, "RheiSchema requires named fields"))?;
            let name_str = name.to_string();
            let kind = FieldKind::from_type(&f.ty)?;
            Ok(FieldInfo {
                name,
                name_str,
                kind,
                col_idx: idx,
            })
        })
        .collect::<Result<Vec<_>, syn::Error>>()?;

    let constants = gen_constants(struct_name, &field_infos);
    let builder = gen_builder(vis, struct_name, &field_infos);
    let view = gen_view(vis, struct_name, &field_infos);
    let columns = gen_columns(vis, struct_name, &field_infos);
    let trait_impl = gen_trait_impl(struct_name, &field_infos);

    Ok(quote! {
        #constants
        #builder
        #view
        #columns
        #trait_impl
    })
}

fn gen_schema_fields(fields: &[FieldInfo]) -> Vec<TokenStream> {
    fields
        .iter()
        .map(|f| {
            let name = &f.name_str;
            let dt = f.kind.arrow_data_type();
            let nullable = f.kind.is_nullable();
            quote! {
                ::arrow_schema::Field::new(#name, #dt, #nullable)
            }
        })
        .collect()
}

fn gen_constants(struct_name: &syn::Ident, fields: &[FieldInfo]) -> TokenStream {
    let consts: Vec<_> = fields
        .iter()
        .map(|f| {
            let const_name = format_ident!("{}", f.name_str.to_uppercase());
            let val = &f.name_str;
            quote! {
                /// Column name constant for use with DataFusion expressions.
                pub const #const_name: &'static str = #val;
            }
        })
        .collect();

    quote! {
        impl #struct_name {
            #(#consts)*
        }
    }
}

fn gen_builder(
    vis: &syn::Visibility,
    struct_name: &syn::Ident,
    fields: &[FieldInfo],
) -> TokenStream {
    let builder_name = format_ident!("{}Builder", struct_name);

    let builder_fields: Vec<_> = fields
        .iter()
        .map(|f| {
            let name = &f.name;
            let ty = f.kind.builder_type();
            quote! { #name: #ty }
        })
        .collect();

    let capacity_ident = quote! { __capacity };
    let builder_inits: Vec<_> = fields
        .iter()
        .map(|f| {
            let name = &f.name;
            let init = f.kind.builder_init(&capacity_ident);
            quote! { #name: #init }
        })
        .collect();

    let append_stmts: Vec<_> = fields
        .iter()
        .map(|f| {
            let name = &f.name;
            let builder_expr = quote! { self.#name };
            let value_expr = quote! { item.#name };
            f.kind.append_value(&builder_expr, &value_expr)
        })
        .collect();

    let null_stmts: Vec<_> = fields
        .iter()
        .map(|f| {
            let name = &f.name;
            let builder_expr = quote! { self.#name };
            f.kind.append_null(&builder_expr)
        })
        .collect();

    let first_field = &fields[0].name;

    let finish_arrays: Vec<_> = fields
        .iter()
        .map(|f| {
            let name = &f.name;
            quote! { ::std::sync::Arc::new(self.#name.finish()) }
        })
        .collect();

    quote! {
        /// Generated columnar builder for [`#struct_name`].
        #[derive(Debug)]
        #vis struct #builder_name {
            #(#builder_fields),*
        }

        impl ::rhei_core::arrow::RheiBuilder for #builder_name {
            type Item = #struct_name;

            fn append(&mut self, item: #struct_name) {
                #(#append_stmts)*
            }

            fn append_null(&mut self) {
                #(#null_stmts)*
            }

            fn len(&self) -> usize {
                ::arrow_array::builder::ArrayBuilder::len(&self.#first_field)
            }

            fn finish(mut self) -> ::arrow_array::RecordBatch {
                ::arrow_array::RecordBatch::try_new(
                    <#struct_name as ::rhei_core::arrow::RheiSchema>::arrow_schema(),
                    vec![#(#finish_arrays),*]
                ).expect("schema and arrays must be compatible")
            }
        }

        impl #builder_name {
            /// Create a new builder with the given row capacity.
            #vis fn with_capacity(#capacity_ident: usize) -> Self {
                Self {
                    #(#builder_inits),*
                }
            }
        }
    }
}

fn gen_view(vis: &syn::Visibility, struct_name: &syn::Ident, fields: &[FieldInfo]) -> TokenStream {
    let view_name = format_ident!("{}View", struct_name);

    let view_fields: Vec<_> = fields
        .iter()
        .map(|f| {
            let name = &f.name;
            let ty = f.kind.view_type();
            quote! { pub #name: #ty }
        })
        .collect();

    let view_extracts: Vec<_> = fields
        .iter()
        .map(|f| {
            let name = &f.name;
            let extract = f.kind.view_extract(f.col_idx);
            quote! { #name: #extract }
        })
        .collect();

    quote! {
        /// Zero-copy view of a single row in a [`#struct_name`] buffer.
        #[derive(Debug, Clone)]
        #vis struct #view_name<'a> {
            #(#view_fields),*,
            #[doc(hidden)]
            pub __phantom: ::std::marker::PhantomData<&'a ()>,
        }

        impl #view_name<'_> {
            #[doc(hidden)]
            pub fn __from_batch(
                batch: &::arrow_array::RecordBatch,
                __row_idx: usize,
            ) -> #view_name<'_> {
                #view_name {
                    #(#view_extracts),*,
                    __phantom: ::std::marker::PhantomData,
                }
            }
        }
    }
}

fn gen_columns(
    vis: &syn::Visibility,
    struct_name: &syn::Ident,
    fields: &[FieldInfo],
) -> TokenStream {
    let columns_name = format_ident!("{}Columns", struct_name);

    let column_fields: Vec<_> = fields
        .iter()
        .map(|f| {
            let name = &f.name;
            let ty = f.kind.column_array_type();
            quote! { pub #name: &'a #ty }
        })
        .collect();

    let column_inits: Vec<_> = fields
        .iter()
        .map(|f| {
            let name = &f.name;
            let downcast = f.kind.column_downcast_expr(f.col_idx);
            quote! { #name: #downcast }
        })
        .collect();

    quote! {
        /// Typed column accessors for vectorized processing of [`#struct_name`] buffers.
        #[derive(Debug)]
        #vis struct #columns_name<'a> {
            #(#column_fields),*
        }

        impl<'a> #columns_name<'a> {
            #[doc(hidden)]
            pub fn __from_batch(batch: &'a ::arrow_array::RecordBatch) -> Self {
                Self {
                    #(#column_inits),*
                }
            }
        }
    }
}

fn gen_trait_impl(struct_name: &syn::Ident, fields: &[FieldInfo]) -> TokenStream {
    let builder_name = format_ident!("{}Builder", struct_name);
    let view_name = format_ident!("{}View", struct_name);
    let columns_name = format_ident!("{}Columns", struct_name);
    let schema_fields = gen_schema_fields(fields);

    quote! {
        impl ::rhei_core::arrow::RheiSchema for #struct_name {
            type Builder = #builder_name;
            type View<'a> = #view_name<'a>;
            type Columns<'a> = #columns_name<'a>;

            fn arrow_schema() -> ::std::sync::Arc<::arrow_schema::Schema> {
                ::std::sync::Arc::new(::arrow_schema::Schema::new(vec![
                    #(#schema_fields),*
                ]))
            }

            fn builder(__capacity: usize) -> Self::Builder {
                #builder_name::with_capacity(__capacity)
            }

            fn view(
                batch: &::arrow_array::RecordBatch,
                index: usize,
            ) -> Self::View<'_> {
                #view_name::__from_batch(batch, index)
            }

            fn columns(batch: &::arrow_array::RecordBatch) -> Self::Columns<'_> {
                #columns_name::__from_batch(batch)
            }
        }
    }
}
