use proc_macro2::Span;
use syn::{GenericArgument, Ident, PathArguments, Type};

/// Convert a `snake_case` identifier to `PascalCase`.
pub(crate) fn snake_to_pascal(name: &str) -> Ident {
    let pascal: String = name
        .split('_')
        .filter(|s| !s.is_empty())
        .map(|seg| {
            let mut chars = seg.chars();
            match chars.next() {
                Some(c) => {
                    let upper: String = c.to_uppercase().collect();
                    upper + chars.as_str()
                }
                None => String::new(),
            }
        })
        .collect();
    Ident::new(&pascal, Span::call_site())
}

/// Extract the inner type `T` from `Vec<T>`.
///
/// Returns `None` if the type is not a `Vec<T>` path.
pub(crate) fn extract_vec_inner(ty: &Type) -> Option<&Type> {
    let Type::Path(type_path) = ty else {
        return None;
    };
    let seg = type_path.path.segments.last()?;
    if seg.ident != "Vec" {
        return None;
    }
    let PathArguments::AngleBracketed(args) = &seg.arguments else {
        return None;
    };
    let first = args.args.first()?;
    let GenericArgument::Type(inner) = first else {
        return None;
    };
    Some(inner)
}

/// Extract the inner type `T` from `Result<Vec<T>>` or `anyhow::Result<Vec<T>>`.
///
/// Specifically, this looks for a path ending in `Result` with a first generic
/// argument that is `Vec<T>`, and returns `T`.
pub(crate) fn extract_result_vec_inner(ty: &Type) -> Option<&Type> {
    let Type::Path(type_path) = ty else {
        return None;
    };
    let seg = type_path.path.segments.last()?;
    if seg.ident != "Result" {
        return None;
    }
    let PathArguments::AngleBracketed(args) = &seg.arguments else {
        return None;
    };
    let first = args.args.first()?;
    let GenericArgument::Type(vec_ty) = first else {
        return None;
    };
    extract_vec_inner(vec_ty)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snake_to_pascal() {
        assert_eq!(snake_to_pascal("word_counter").to_string(), "WordCounter");
        assert_eq!(snake_to_pascal("batch_upper").to_string(), "BatchUpper");
        assert_eq!(snake_to_pascal("simple").to_string(), "Simple");
        assert_eq!(snake_to_pascal("a_b_c").to_string(), "ABC");
        assert_eq!(snake_to_pascal("my_long_name").to_string(), "MyLongName");
    }

    #[test]
    fn test_extract_vec_inner() {
        let ty: Type = syn::parse_str("Vec<String>").unwrap();
        let inner = extract_vec_inner(&ty).unwrap();
        let expected: Type = syn::parse_str("String").unwrap();
        assert_eq!(
            quote::quote!(#inner).to_string(),
            quote::quote!(#expected).to_string()
        );
    }

    #[test]
    fn test_extract_result_vec_inner() {
        let ty: Type = syn::parse_str("anyhow::Result<Vec<String>>").unwrap();
        let inner = extract_result_vec_inner(&ty).unwrap();
        let expected: Type = syn::parse_str("String").unwrap();
        assert_eq!(
            quote::quote!(#inner).to_string(),
            quote::quote!(#expected).to_string()
        );
    }

    #[test]
    fn test_extract_result_vec_inner_plain() {
        let ty: Type = syn::parse_str("Result<Vec<u64>>").unwrap();
        let inner = extract_result_vec_inner(&ty).unwrap();
        let expected: Type = syn::parse_str("u64").unwrap();
        assert_eq!(
            quote::quote!(#inner).to_string(),
            quote::quote!(#expected).to_string()
        );
    }
}
