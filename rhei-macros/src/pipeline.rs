use proc_macro2::TokenStream;
use quote::quote;
use syn::{Error, ItemFn};

pub(crate) fn expand(item: ItemFn) -> Result<TokenStream, Error> {
    // Must NOT be async
    if item.sig.asyncness.is_some() {
        return Err(Error::new_spanned(
            item.sig.fn_token,
            "#[pipeline] function must not be async (the macro adds #[tokio::main])",
        ));
    }

    // Must be named "main"
    if item.sig.ident != "main" {
        return Err(Error::new_spanned(
            &item.sig.ident,
            "#[pipeline] function must be named `main`",
        ));
    }

    // Must have exactly 1 param (the graph ref)
    let params: Vec<_> = item.sig.inputs.iter().collect();
    if params.len() != 1 {
        return Err(Error::new_spanned(
            &item.sig.inputs,
            "#[pipeline] function must have exactly 1 parameter: (graph: &DataflowGraph)",
        ));
    }

    let first_param = &params[0];
    let syn::FnArg::Typed(first_pat) = first_param else {
        return Err(Error::new_spanned(
            first_param,
            "#[pipeline] function must not have a self parameter",
        ));
    };
    let graph_pat = &*first_pat.pat;

    let body = &item.block;

    Ok(quote! {
        #[::rhei::__private::tokio::main]
        async fn main() -> ::rhei::__private::anyhow::Result<()> {
            let __rhei_dir = ::std::env::var("RHEI_CHECKPOINT_DIR")
                .map(::std::path::PathBuf::from)
                .unwrap_or_else(|_| ::std::env::temp_dir().join("rhei_pipeline"));
            ::std::fs::create_dir_all(&__rhei_dir)?;

            let __rhei_graph = ::rhei::DataflowGraph::new();
            {
                let #graph_pat = &__rhei_graph;
                #body
            }

            ::rhei::PipelineController::builder()
                .checkpoint_dir(&__rhei_dir)
                .from_env()
                .build()
                .start(__rhei_graph)
                .await?;
            Ok(())
        }
    })
}
