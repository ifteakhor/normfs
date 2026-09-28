use crate::client::S3Client;
use crate::errors::CloudError;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use uintn::UintN;

fn path_to_id(path: &str, extension: &str) -> Result<UintN, CloudError> {
    let mut hex_string = String::new();
    for component in path.split('/') {
        if let Some(stripped) = component.strip_suffix(&format!(".{}", extension)) {
            hex_string.push_str(stripped);
        } else if !component.is_empty() {
            hex_string.push_str(component);
        }
    }
    Ok(UintN::from_hex_digits(&hex_string)?)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum End {
    Min,
    Max,
}

/// `UintN::to_file_path` pads every component to three lowercase hex digits,
/// so at one level lexicographic order is numeric order. Anything else under
/// the prefix is not one of ours.
pub fn is_id_component(name: &str) -> bool {
    name.len() == 3 && name.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// An id path relative to the queue prefix, and how many components it has.
/// More components is a larger id: the leading one is never `000`.
type Found = (String, usize);

/// Every id below a directory has more components than the files beside it,
/// so a file on a level is smaller than anything under that level's
/// directories. Between directories the name alone does not decide:
/// `002/fff` is smaller than `001/000/000`. So the smallest id is a file here
/// if there is one, and otherwise the shallowest, then first, among the
/// directories; the largest is the deepest, then last. `bound` is the depth a
/// candidate must beat, which is what lets a shallow subtree be dropped.
fn find_id_recursive<'a>(
    client: &'a Arc<S3Client>,
    prefix: &'a str,
    extension: &'a str,
    end: End,
    level: usize,
    bound: Option<usize>,
) -> Pin<Box<dyn Future<Output = Result<Option<Found>, CloudError>> + Send + 'a>> {
    Box::pin(async move {
        if end == End::Min && bound.is_some_and(|b| level + 1 >= b) {
            return Ok(None);
        }

        let listing = client.list_objects(prefix, Some("/")).await?;
        let suffix = format!(".{extension}");

        let mut files: Vec<&str> = listing
            .contents
            .iter()
            .filter_map(|o| o.key.strip_prefix(prefix)?.strip_suffix(suffix.as_str()))
            .filter(|stem| is_id_component(stem))
            .collect();
        let mut dirs: Vec<&str> = listing
            .common_prefixes
            .iter()
            .filter_map(|p| p.prefix.strip_prefix(prefix)?.strip_suffix('/'))
            .filter(|name| is_id_component(name))
            .collect();
        files.sort_unstable();
        dirs.sort_unstable();

        let here = |file: &&str| (format!("{file}{suffix}"), level + 1);
        if end == End::Min
            && let Some(file) = files.first()
        {
            return Ok(Some(here(file)));
        }

        if end == End::Max {
            dirs.reverse();
        }
        let mut best: Option<Found> = None;
        for dir in dirs {
            let sub_prefix = format!("{prefix}{dir}/");
            let sub_bound = best.as_ref().map(|(_, depth)| *depth).or(bound);
            if let Some((path, depth)) =
                find_id_recursive(client, &sub_prefix, extension, end, level + 1, sub_bound).await?
            {
                best = Some((format!("{dir}/{path}"), depth));
            }
        }

        if best.is_none() && end == End::Max && bound.is_none_or(|b| level + 1 > b) {
            best = files.last().map(here);
        }
        Ok(best)
    })
}

async fn find_id(
    client: &Arc<S3Client>,
    prefix: &str,
    extension: &str,
    end: End,
) -> Result<UintN, CloudError> {
    let (path, _) = find_id_recursive(client, prefix, extension, end, 0, None)
        .await?
        .ok_or(CloudError::NoFilesFound)?;
    path_to_id(&path, extension)
}

pub async fn find_min_id(
    client: &Arc<S3Client>,
    prefix: &str,
    extension: &str,
) -> Result<UintN, CloudError> {
    find_id(client, prefix, extension, End::Min).await
}

/// One LIST per level for the smallest id in a contiguous range; the largest
/// costs one per directory, since a shallow subtree is only known to be
/// shallow once listed.
pub async fn find_max_id(
    client: &Arc<S3Client>,
    prefix: &str,
    extension: &str,
) -> Result<UintN, CloudError> {
    find_id(client, prefix, extension, End::Max).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_to_id() {
        let id = path_to_id("12/34/56.store", "store").unwrap();
        assert_eq!(id, UintN::from_hex_digits("123456").unwrap());

        let id = path_to_id("a/b/c/d.store", "store").unwrap();
        assert_eq!(id, UintN::from_hex_digits("abcd").unwrap());

        let id = path_to_id("12/3456.store", "store").unwrap();
        assert_eq!(id, UintN::from_hex_digits("123456").unwrap());

        let id = path_to_id("123456.store", "store").unwrap();
        assert_eq!(id, UintN::from_hex_digits("123456").unwrap());
    }
}
