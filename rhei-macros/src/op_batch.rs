use proc_macro2::TokenStream;
use quote::quote;
use syn::{Error, ItemFn, ReturnType};

use crate::util::{extract_generic_inner, extract_result_buffer_output_inner, snake_to_pascal};

pub(crate) fn expand(item: ItemFn) -> Result<TokenStream, Error> {
    if item.sig.asyncness.is_none() {
        return Err(Error::new_spanned(
            item.sig.fn_token,
            "#[op_batch] function must be async",
        ));
    }

    let params: Vec<_> = item.sig.inputs.iter().collect();
    if params.len() != 2 {
        return Err(Error::new_spanned(
            &item.sig.inputs,
            "#[op_batch] function must have exactly 2 parameters: \
             (input: RheiBuffer<I>, ctx: &mut OperatorContext)",
        ));
    }

    // Extract input type from first param — must be RheiBuffer<T>
    let first_param = &params[0];
    let syn::FnArg::Typed(first_pat) = first_param else {
        return Err(Error::new_spanned(
            first_param,
            "#[op_batch] function must not have a self parameter",
        ));
    };
    let input_buf_type = &*first_pat.ty;
    let input_buf_pat = &*first_pat.pat;

    let input_type = extract_generic_inner(input_buf_type, "RheiBuffer").ok_or_else(|| {
        Error::new_spanned(
            input_buf_type,
            "#[op_batch] first parameter must be RheiBuffer<T>",
        )
    })?;

    // Extract ctx param
    let second_param = &params[1];
    let syn::FnArg::Typed(second_pat) = second_param else {
        return Err(Error::new_spanned(
            second_param,
            "#[op_batch] function must not have a self parameter",
        ));
    };
    let ctx_pat = &*second_pat.pat;
    let ctx_type = &*second_pat.ty;

    // Extract return type — must be Result<BufferOutput<T>>
    let ReturnType::Type(_, ref return_ty) = item.sig.output else {
        return Err(Error::new_spanned(
            &item.sig,
            "#[op_batch] function must return anyhow::Result<BufferOutput<T>>",
        ));
    };

    let output_type = extract_result_buffer_output_inner(return_ty).ok_or_else(|| {
        Error::new_spanned(
            return_ty,
            "#[op_batch] function must return anyhow::Result<BufferOutput<T>>",
        )
    })?;

    let return_type = &**return_ty;
    let struct_name = snake_to_pascal(&item.sig.ident.to_string());
    let body = &item.block;

    Ok(quote! {
        #[derive(Clone, Debug)]
        struct #struct_name;

        #[::rhei::__private::async_trait]
        impl ::rhei::StreamFunction for #struct_name {
            type Input = #input_type;
            type Output = #output_type;

            async fn process(
                &mut self,
                #input_buf_pat: #input_buf_type,
                #ctx_pat: #ctx_type,
            ) -> #return_type {
                #body
            }
        }
    })
}
