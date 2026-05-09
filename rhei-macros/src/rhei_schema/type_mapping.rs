use proc_macro2::TokenStream;
use quote::quote;
use syn::{GenericArgument, Path, PathArguments, Type, TypePath};

#[derive(Debug, Clone)]
pub(crate) enum FieldKind {
    Primitive(PrimitiveKind),
    StringType,
    ByteVec,
    Optional(Box<FieldKind>),
    List(Box<FieldKind>),
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum PrimitiveKind {
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
    F32,
    F64,
    Bool,
}

impl FieldKind {
    pub(crate) fn from_type(ty: &Type) -> Result<Self, syn::Error> {
        match ty {
            Type::Path(TypePath { path, .. }) => Self::from_path(path),
            _ => Err(syn::Error::new_spanned(
                ty,
                "unsupported type for RheiSchema",
            )),
        }
    }

    fn from_path(path: &Path) -> Result<Self, syn::Error> {
        let segment = path
            .segments
            .last()
            .ok_or_else(|| syn::Error::new_spanned(path, "empty path"))?;
        let ident = segment.ident.to_string();

        match ident.as_str() {
            "i8" => Ok(Self::Primitive(PrimitiveKind::I8)),
            "i16" => Ok(Self::Primitive(PrimitiveKind::I16)),
            "i32" => Ok(Self::Primitive(PrimitiveKind::I32)),
            "i64" => Ok(Self::Primitive(PrimitiveKind::I64)),
            "u8" => Ok(Self::Primitive(PrimitiveKind::U8)),
            "u16" => Ok(Self::Primitive(PrimitiveKind::U16)),
            "u32" => Ok(Self::Primitive(PrimitiveKind::U32)),
            "u64" => Ok(Self::Primitive(PrimitiveKind::U64)),
            "f32" => Ok(Self::Primitive(PrimitiveKind::F32)),
            "f64" => Ok(Self::Primitive(PrimitiveKind::F64)),
            "bool" => Ok(Self::Primitive(PrimitiveKind::Bool)),
            "String" => Ok(Self::StringType),
            "Option" => {
                let inner = extract_single_generic(&segment.arguments)?;
                let inner_kind = Self::from_type(&inner)?;
                Ok(Self::Optional(Box::new(inner_kind)))
            }
            "Vec" => {
                let inner = extract_single_generic(&segment.arguments)?;
                if is_u8_type(&inner) {
                    return Ok(Self::ByteVec);
                }
                let inner_kind = Self::from_type(&inner)?;
                Ok(Self::List(Box::new(inner_kind)))
            }
            other => Err(syn::Error::new_spanned(
                path,
                format!(
                    "unsupported type `{other}` for RheiSchema; supported: i8-i64, u8-u64, f32, f64, bool, String, Option<T>, Vec<T>"
                ),
            )),
        }
    }

    pub(crate) fn arrow_data_type(&self) -> TokenStream {
        match self {
            Self::Primitive(p) => p.arrow_data_type(),
            Self::StringType => quote! { ::arrow_schema::DataType::Utf8 },
            Self::ByteVec => quote! { ::arrow_schema::DataType::Binary },
            Self::Optional(inner) => inner.arrow_data_type(),
            Self::List(inner) => {
                let inner_dt = inner.arrow_data_type();
                quote! {
                    ::arrow_schema::DataType::List(
                        ::std::sync::Arc::new(::arrow_schema::Field::new("item", #inner_dt, true))
                    )
                }
            }
        }
    }

    pub(crate) fn is_nullable(&self) -> bool {
        matches!(self, Self::Optional(_))
    }

    pub(crate) fn builder_type(&self) -> TokenStream {
        match self {
            Self::Primitive(p) => p.builder_type(),
            Self::StringType => quote! { ::arrow_array::builder::StringBuilder },
            Self::ByteVec => quote! { ::arrow_array::builder::BinaryBuilder },
            Self::Optional(inner) => inner.builder_type(),
            Self::List(inner) => {
                let inner_builder = inner.builder_type();
                quote! { ::arrow_array::builder::ListBuilder<#inner_builder> }
            }
        }
    }

    pub(crate) fn builder_init(&self, capacity: &TokenStream) -> TokenStream {
        match self {
            Self::Primitive(p) => p.builder_init(capacity),
            Self::StringType => {
                quote! { ::arrow_array::builder::StringBuilder::with_capacity(#capacity, #capacity * 32) }
            }
            Self::ByteVec => {
                quote! { ::arrow_array::builder::BinaryBuilder::with_capacity(#capacity, #capacity * 64) }
            }
            Self::Optional(inner) => inner.builder_init(capacity),
            Self::List(inner) => {
                let inner_init = inner.builder_init(capacity);
                quote! { ::arrow_array::builder::ListBuilder::new(#inner_init) }
            }
        }
    }

    pub(crate) fn append_value(
        &self,
        builder_expr: &TokenStream,
        value_expr: &TokenStream,
    ) -> TokenStream {
        match self {
            Self::Primitive(_) | Self::StringType => {
                quote! { #builder_expr.append_value(#value_expr); }
            }
            Self::ByteVec => {
                quote! { #builder_expr.append_value(&#value_expr); }
            }
            Self::Optional(inner) => {
                let some_append = inner.append_value(builder_expr, &quote! { v });
                quote! {
                    match #value_expr {
                        Some(v) => { #some_append }
                        None => { #builder_expr.append_null(); }
                    }
                }
            }
            Self::List(inner) => {
                let values_builder = quote! { #builder_expr.values() };
                let item_append = inner.append_value(&values_builder, &quote! { item });
                quote! {
                    for item in #value_expr {
                        #item_append
                    }
                    #builder_expr.append(true);
                }
            }
        }
    }

    pub(crate) fn append_null(&self, builder_expr: &TokenStream) -> TokenStream {
        match self {
            Self::List(_) => quote! { #builder_expr.append(false); },
            _ => quote! { #builder_expr.append_null(); },
        }
    }

    pub(crate) fn view_type(&self) -> TokenStream {
        match self {
            Self::Primitive(p) => p.rust_type(),
            Self::StringType => quote! { &'a str },
            Self::ByteVec => quote! { &'a [u8] },
            Self::Optional(inner) => {
                let inner_vt = inner.view_type();
                quote! { Option<#inner_vt> }
            }
            Self::List(inner) => {
                let inner_vt = inner.owned_type();
                quote! { Vec<#inner_vt> }
            }
        }
    }

    fn owned_type(&self) -> TokenStream {
        match self {
            Self::Primitive(p) => p.rust_type(),
            Self::StringType => quote! { String },
            Self::ByteVec => quote! { Vec<u8> },
            Self::Optional(inner) => {
                let inner_owned = inner.owned_type();
                quote! { Option<#inner_owned> }
            }
            Self::List(inner) => {
                let inner_owned = inner.owned_type();
                quote! { Vec<#inner_owned> }
            }
        }
    }

    pub(crate) fn view_extract(&self, col_idx: usize) -> TokenStream {
        let idx = syn::Index::from(col_idx);
        match self {
            Self::Primitive(p) => p.view_extract_at(col_idx),
            Self::StringType => {
                quote! {
                    batch.column(#idx)
                        .as_any()
                        .downcast_ref::<::arrow_array::StringArray>()
                        .expect("column type mismatch")
                        .value(__row_idx)
                }
            }
            Self::ByteVec => {
                quote! {
                    batch.column(#idx)
                        .as_any()
                        .downcast_ref::<::arrow_array::BinaryArray>()
                        .expect("column type mismatch")
                        .value(__row_idx)
                }
            }
            Self::Optional(inner) => {
                let col = quote! { batch.column(#idx) };
                let value_extract = match inner.as_ref() {
                    FieldKind::Primitive(PrimitiveKind::Bool) => quote! {
                        #col.as_any()
                            .downcast_ref::<::arrow_array::BooleanArray>()
                            .expect("column type mismatch")
                            .value(__row_idx)
                    },
                    FieldKind::Primitive(p) => {
                        let arrow_type = p.arrow_primitive_type();
                        quote! {
                            #col.as_any()
                                .downcast_ref::<::arrow_array::PrimitiveArray<#arrow_type>>()
                                .expect("column type mismatch")
                                .value(__row_idx)
                        }
                    }
                    FieldKind::StringType => quote! {
                        #col.as_any()
                            .downcast_ref::<::arrow_array::StringArray>()
                            .expect("column type mismatch")
                            .value(__row_idx)
                    },
                    FieldKind::ByteVec => quote! {
                        #col.as_any()
                            .downcast_ref::<::arrow_array::BinaryArray>()
                            .expect("column type mismatch")
                            .value(__row_idx)
                    },
                    _ => quote! { todo!("nested optional extraction") },
                };
                quote! {
                    if batch.column(#idx).is_null(__row_idx) {
                        None
                    } else {
                        Some(#value_extract)
                    }
                }
            }
            Self::List(inner) => {
                let inner_extract = inner.list_value_extract();
                quote! {
                    {
                        let __list_arr = batch.column(#idx)
                            .as_any()
                            .downcast_ref::<::arrow_array::ListArray>()
                            .expect("column type mismatch");
                        let __offsets = __list_arr.value_offsets();
                        let __start = __offsets[__row_idx] as usize;
                        let __end = __offsets[__row_idx + 1] as usize;
                        let __values = __list_arr.values();
                        (__start..__end).map(|__i| {
                            #inner_extract
                        }).collect::<Vec<_>>()
                    }
                }
            }
        }
    }

    #[allow(dead_code)]
    fn column_downcast(&self, col_idx: usize) -> TokenStream {
        let idx = syn::Index::from(col_idx);
        match self {
            Self::Primitive(PrimitiveKind::Bool) => quote! {
                batch.column(#idx)
                    .as_any()
                    .downcast_ref::<::arrow_array::BooleanArray>()
                    .expect("column type mismatch")
            },
            Self::Primitive(p) => {
                let arrow_type = p.arrow_primitive_type();
                quote! {
                    batch.column(#idx)
                        .as_any()
                        .downcast_ref::<::arrow_array::PrimitiveArray<#arrow_type>>()
                        .expect("column type mismatch")
                }
            }
            Self::StringType => quote! {
                batch.column(#idx)
                    .as_any()
                    .downcast_ref::<::arrow_array::StringArray>()
                    .expect("column type mismatch")
            },
            Self::ByteVec => quote! {
                batch.column(#idx)
                    .as_any()
                    .downcast_ref::<::arrow_array::BinaryArray>()
                    .expect("column type mismatch")
            },
            _ => quote! { batch.column(#idx) },
        }
    }

    #[allow(dead_code)]
    fn value_at_row(&self) -> TokenStream {
        match self {
            Self::Primitive(_) => quote! { __col.value(__row_idx) },
            Self::StringType => quote! { __col.value(__row_idx) },
            Self::ByteVec => quote! { __col.value(__row_idx) },
            _ => quote! { unreachable!() },
        }
    }

    fn list_value_extract(&self) -> TokenStream {
        match self {
            Self::Primitive(p) => {
                let arrow_type = p.arrow_primitive_type();
                quote! {
                    __values.as_any()
                        .downcast_ref::<::arrow_array::PrimitiveArray<#arrow_type>>()
                        .expect("list values type mismatch")
                        .value(__i)
                }
            }
            Self::StringType => quote! {
                __values.as_any()
                    .downcast_ref::<::arrow_array::StringArray>()
                    .expect("list values type mismatch")
                    .value(__i)
                    .to_string()
            },
            _ => quote! { todo!("nested list extraction") },
        }
    }

    pub(crate) fn column_array_type(&self) -> TokenStream {
        match self {
            Self::Primitive(PrimitiveKind::Bool) => quote! { ::arrow_array::BooleanArray },
            Self::Primitive(p) => {
                let arrow_type = p.arrow_primitive_type();
                quote! { ::arrow_array::PrimitiveArray<#arrow_type> }
            }
            Self::StringType => quote! { ::arrow_array::StringArray },
            Self::ByteVec => quote! { ::arrow_array::BinaryArray },
            Self::Optional(inner) => inner.column_array_type(),
            Self::List(_) => quote! { ::arrow_array::ListArray },
        }
    }

    pub(crate) fn column_downcast_expr(&self, col_idx: usize) -> TokenStream {
        let idx = syn::Index::from(col_idx);
        let arr_type = self.column_array_type();
        quote! {
            batch.column(#idx)
                .as_any()
                .downcast_ref::<#arr_type>()
                .expect("column type mismatch")
        }
    }
}

impl PrimitiveKind {
    fn arrow_data_type(self) -> TokenStream {
        match self {
            Self::I8 => quote! { ::arrow_schema::DataType::Int8 },
            Self::I16 => quote! { ::arrow_schema::DataType::Int16 },
            Self::I32 => quote! { ::arrow_schema::DataType::Int32 },
            Self::I64 => quote! { ::arrow_schema::DataType::Int64 },
            Self::U8 => quote! { ::arrow_schema::DataType::UInt8 },
            Self::U16 => quote! { ::arrow_schema::DataType::UInt16 },
            Self::U32 => quote! { ::arrow_schema::DataType::UInt32 },
            Self::U64 => quote! { ::arrow_schema::DataType::UInt64 },
            Self::F32 => quote! { ::arrow_schema::DataType::Float32 },
            Self::F64 => quote! { ::arrow_schema::DataType::Float64 },
            Self::Bool => quote! { ::arrow_schema::DataType::Boolean },
        }
    }

    fn builder_type(self) -> TokenStream {
        match self {
            Self::Bool => quote! { ::arrow_array::builder::BooleanBuilder },
            _ => {
                let prim_type = self.arrow_primitive_type();
                quote! { ::arrow_array::builder::PrimitiveBuilder<#prim_type> }
            }
        }
    }

    fn builder_init(self, capacity: &TokenStream) -> TokenStream {
        match self {
            Self::Bool => {
                quote! { ::arrow_array::builder::BooleanBuilder::with_capacity(#capacity) }
            }
            _ => {
                let prim_type = self.arrow_primitive_type();
                quote! { ::arrow_array::builder::PrimitiveBuilder::<#prim_type>::with_capacity(#capacity) }
            }
        }
    }

    fn arrow_primitive_type(self) -> TokenStream {
        match self {
            Self::I8 => quote! { ::arrow_array::types::Int8Type },
            Self::I16 => quote! { ::arrow_array::types::Int16Type },
            Self::I32 => quote! { ::arrow_array::types::Int32Type },
            Self::I64 => quote! { ::arrow_array::types::Int64Type },
            Self::U8 => quote! { ::arrow_array::types::UInt8Type },
            Self::U16 => quote! { ::arrow_array::types::UInt16Type },
            Self::U32 => quote! { ::arrow_array::types::UInt32Type },
            Self::U64 => quote! { ::arrow_array::types::UInt64Type },
            Self::F32 => quote! { ::arrow_array::types::Float32Type },
            Self::F64 => quote! { ::arrow_array::types::Float64Type },
            Self::Bool => unreachable!("Bool is not a PrimitiveArray type"),
        }
    }

    pub(crate) fn view_extract_at(self, col_idx: usize) -> TokenStream {
        let idx = syn::Index::from(col_idx);
        match self {
            Self::Bool => quote! {
                batch.column(#idx)
                    .as_any()
                    .downcast_ref::<::arrow_array::BooleanArray>()
                    .expect("column type mismatch")
                    .value(__row_idx)
            },
            _ => {
                let arrow_type = self.arrow_primitive_type();
                quote! {
                    batch.column(#idx)
                        .as_any()
                        .downcast_ref::<::arrow_array::PrimitiveArray<#arrow_type>>()
                        .expect("column type mismatch")
                        .value(__row_idx)
                }
            }
        }
    }

    fn rust_type(self) -> TokenStream {
        match self {
            Self::I8 => quote! { i8 },
            Self::I16 => quote! { i16 },
            Self::I32 => quote! { i32 },
            Self::I64 => quote! { i64 },
            Self::U8 => quote! { u8 },
            Self::U16 => quote! { u16 },
            Self::U32 => quote! { u32 },
            Self::U64 => quote! { u64 },
            Self::F32 => quote! { f32 },
            Self::F64 => quote! { f64 },
            Self::Bool => quote! { bool },
        }
    }
}

fn extract_single_generic(args: &PathArguments) -> Result<Type, syn::Error> {
    match args {
        PathArguments::AngleBracketed(ab) => {
            let arg = ab
                .args
                .first()
                .ok_or_else(|| syn::Error::new_spanned(ab, "expected one generic argument"))?;
            match arg {
                GenericArgument::Type(ty) => Ok(ty.clone()),
                _ => Err(syn::Error::new_spanned(arg, "expected a type argument")),
            }
        }
        _ => Err(syn::Error::new_spanned(
            &proc_macro2::TokenStream::new(),
            "expected angle-bracketed generic arguments",
        )),
    }
}

fn is_u8_type(ty: &Type) -> bool {
    if let Type::Path(TypePath { path, .. }) = ty {
        if let Some(seg) = path.segments.last() {
            return seg.ident == "u8" && seg.arguments.is_empty();
        }
    }
    false
}
