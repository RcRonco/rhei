use proc_macro2::TokenStream;
use quote::quote;
use syn::{Error, ItemFn, ReturnType};

use crate::util::{extract_result_vec_inner, snake_to_pascal};

pub(crate) fn expand(item: ItemFn) -> Result<TokenStream, Error> {
    // Must be async
    if item.sig.asyncness.is_none() {
        return Err(Error::new_spanned(
            item.sig.fn_token,
            "#[op] function must be async",
        ));
    }

    // Must have exactly 2 params (no self)
    let params: Vec<_> = item.sig.inputs.iter().collect();
    if params.len() != 2 {
        return Err(Error::new_spanned(
            &item.sig.inputs,
            "#[op] function must have exactly 2 parameters: (input: T, ctx: &mut StateContext)",
        ));
    }

    // Extract input type from first param
    let first_param = &params[0];
    let syn::FnArg::Typed(first_pat) = first_param else {
        return Err(Error::new_spanned(
            first_param,
            "#[op] function must not have a self parameter",
        ));
    };
    let input_type = &*first_pat.ty;
    let input_pat = &*first_pat.pat;

    // Extract ctx param
    let second_param = &params[1];
    let syn::FnArg::Typed(second_pat) = second_param else {
        return Err(Error::new_spanned(
            second_param,
            "#[op] function must not have a self parameter",
        ));
    };
    let ctx_pat = &*second_pat.pat;
    let ctx_type = &*second_pat.ty;

    // Extract return type — must be Result<Vec<T>>
    let ReturnType::Type(_, ref return_ty) = item.sig.output else {
        return Err(Error::new_spanned(
            &item.sig,
            "#[op] function must return anyhow::Result<Vec<T>>",
        ));
    };

    let output_type = extract_result_vec_inner(return_ty).ok_or_else(|| {
        Error::new_spanned(
            return_ty,
            "#[op] function must return anyhow::Result<Vec<T>>",
        )
    })?;

    let return_type = &**return_ty;

    // Generate struct name from function name
    let struct_name = snake_to_pascal(&item.sig.ident.to_string());
    let vis = &item.vis;

    let body = &item.block;

    Ok(quote! {
        #[derive(Clone, Debug)]
        #vis struct #struct_name;

        #[::rhei::__private::async_trait]
        impl ::rhei::StreamFunction for #struct_name {
            type Input = #input_type;
            type Output = #output_type;

            async fn process(
                &mut self,
                #input_pat: #input_type,
                #ctx_pat: #ctx_type,
            ) -> #return_type {
                #body
            }
        }
    })
}
