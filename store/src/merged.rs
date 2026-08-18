//! How a merged file records the files it replaced.
//!
//! A merged file is named `<own id>_<replaced id>_<replaced id>…`, so a reader
//! can tell from a listing alone which are stale, without opening anything.
//! That name is what commits a merge: the upload is invisible until it
//! completes, and once visible it already declares its sources replaced.

use std::collections::HashSet;

use datafusion::object_store::ObjectMeta;
use uuid::Uuid;

/// Delimits the ids packed into a merged filename. Ids are written
/// unhyphenated, so anything outside hex separates them.
const SEPARATOR: char = '_';

/// Splits a listing into what is live and what something else in it already
/// replaced. The replaced ones are present only because a delete has yet to
/// run, and their rows are in the merged file too, so anything reading them
/// counts those rows twice.
pub fn split_live(listing: Vec<ObjectMeta>) -> (Vec<ObjectMeta>, Vec<ObjectMeta>) {
    let stale: HashSet<String> = listing
        .iter()
        .filter_map(|file| file.location.filename())
        .flat_map(superseded)
        .map(String::from)
        .collect();

    listing.into_iter().partition(|file| {
        file.location
            .filename()
            .is_some_and(|name| !stale.contains(own_id(name)))
    })
}

/// Ids a merged file replaced, read from its name. Anything listed here is
/// stale even if still present, so a reader must exclude it.
pub fn superseded(name: &str) -> Vec<&str> {
    name.strip_suffix(".parquet")
        .unwrap_or(name)
        .split(SEPARATOR)
        .skip(1)
        .collect()
}

/// A new id, then each source's own — not the ids those in turn replaced, or
/// the name would grow at every level until it passed the key limit.
pub fn filename(sources: &[ObjectMeta]) -> String {
    let mut name = Uuid::now_v7().simple().to_string();
    for source in sources {
        name.push(SEPARATOR);
        name.push_str(own_id(
            source.location.filename().expect("sources are files"),
        ));
    }
    name.push_str(".parquet");
    name
}

fn own_id(name: &str) -> &str {
    name.strip_suffix(".parquet")
        .unwrap_or(name)
        .split(SEPARATOR)
        .next()
        .expect("split yields at least one field")
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use datafusion::object_store::path::Path;

    fn meta(name: &str) -> ObjectMeta {
        ObjectMeta {
            location: Path::from(format!("partition/{name}")),
            last_modified: Utc::now(),
            size: 1,
            e_tag: None,
            version: None,
        }
    }

    fn names(files: &[ObjectMeta]) -> Vec<String> {
        files
            .iter()
            .map(|file| file.location.filename().unwrap().to_string())
            .collect()
    }

    /// A source id landing in the own-id slot would make the output look like
    /// it replaces one fewer file than it does.
    #[test]
    fn superseded_skips_the_files_own_id() {
        assert_eq!(superseded("self.parquet"), Vec::<&str>::new());
        assert_eq!(superseded("self_a_b.parquet"), vec!["a", "b"]);
    }

    /// A failed delete leaves sources listed beside the merged file that
    /// already holds their rows.
    #[test]
    fn split_live_separates_replaced_files() {
        let listing = vec![meta("a.parquet"), meta("b.parquet"), meta("m_a_b.parquet")];

        let (live, stale) = split_live(listing);
        assert_eq!(names(&live), vec!["m_a_b.parquet"]);
        assert_eq!(names(&stale), vec!["a.parquet", "b.parquet"]);
    }

    /// Merging merged files is the normal case after the first pass, so a name
    /// that grew at every level would pass the key limit by level three.
    #[test]
    fn merged_names_do_not_compound() {
        let first = filename(&[meta("a.parquet"), meta("b.parquet")]);
        let second = filename(&[meta(&first), meta("c.parquet")]);

        assert_eq!(superseded(&second).len(), 2, "{second}");
    }
}
