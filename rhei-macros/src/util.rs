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

/// Extract the inner type `T` from a generic wrapper like `RheiBuffer<T>`.
///
/// Matches any path whose last segment has the given `wrapper_name` and one
/// type argument.
pub(crate) fn extract_generic_inner<'a>(ty: &'a Type, wrapper_name: &str) -> Option<&'a Type> {
    let Type::Path(type_path) = ty else {
        return None;
    };
    let seg = type_path.path.segments.last()?;
    if seg.ident != wrapper_name {
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

/// Extract type `T` from `Result<BufferOutput<T>>` or `anyhow::Result<BufferOutput<T>>`.
pub(crate) fn extract_result_buffer_output_inner(ty: &Type) -> Option<&Type> {
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
    let GenericArgument::Type(buf_out_ty) = first else {
        return None;
    };
    extract_generic_inner(buf_out_ty, "BufferOutput")
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
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
}
