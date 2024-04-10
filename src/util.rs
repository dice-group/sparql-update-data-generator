use std::path::{Path, PathBuf};

pub fn dataset_iter(
    paths: Vec<PathBuf>,
    recursive: bool,
    extension: &str,
) -> impl Iterator<Item = walkdir::Result<PathBuf>> + '_ {
    paths.into_iter().flat_map(move |path| {
        if path.is_dir() {
            if recursive {
                walkdir::WalkDir::new(path)
                    .into_iter()
                    .filter_map(|e| match e {
                        Ok(e)
                            if e.file_type().is_file()
                                && matches!(e.path().extension(), Some(ext) if ext == extension) =>
                        {
                            Some(Ok(e.into_path()))
                        },
                        Ok(_) => None,
                        other => Some(other.map(|e| e.into_path())),
                    })
                    .collect()
            } else {
                vec![]
            }
        } else {
            vec![Ok(path)]
        }
    })
}

pub fn changeset_file_iter<P: AsRef<Path>>(
    path: P,
    extension: &str,
) -> impl Iterator<Item = walkdir::Result<walkdir::DirEntry>> + '_ {
    walkdir::WalkDir::new(path.as_ref())
        .sort_by_file_name()
        .into_iter()
        .filter(move |de| {
            de.as_ref()
                .map(|de| de.file_type().is_file() && matches!(de.path().extension(), Some(ext) if ext == extension))
                .unwrap_or(true)
        })
}
