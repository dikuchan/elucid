use std::env;
use std::error::Error;
use std::fmt::Write as _;
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};

const ASSET_DIRECTORY: &str = "ui-assets";
const GENERATED_MODULE: &str = "embedded_ui_assets.rs";
const MAXIMUM_ASSET_FILES: usize = 128;
const MAXIMUM_ASSET_BYTES: u64 = 16 * 1_024 * 1_024;

fn main() -> Result<(), Box<dyn Error>> {
    let manifest_directory = required_directory("CARGO_MANIFEST_DIR")?;
    let output_directory = required_directory("OUT_DIR")?;
    let asset_root = manifest_directory.join(ASSET_DIRECTORY);
    println!("cargo:rerun-if-changed={}", asset_root.display());
    if !asset_root.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "embedded UI assets are missing at {}; run `make ui-assets` from the repository root",
                asset_root.display()
            ),
        )
        .into());
    }

    let assets = collect_assets(&asset_root)?;
    if !assets.iter().any(|path| path == Path::new("index.html")) {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("{} must contain index.html", asset_root.display()),
        )
        .into());
    }

    let mut source = String::from("&[\n");
    for relative_path in assets {
        let absolute_path = asset_root.join(&relative_path);
        let asset_name = slash_separated(&relative_path)?;
        let absolute_path = absolute_path.to_str().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "UI asset absolute path is not UTF-8",
            )
        })?;
        writeln!(
            source,
            "    EmbeddedAsset {{ path: {asset_name:?}, bytes: include_bytes!({absolute_path:?}) }},",
        )?;
    }
    source.push_str("]\n");
    fs::write(output_directory.join(GENERATED_MODULE), source)?;
    Ok(())
}

fn required_directory(name: &str) -> io::Result<PathBuf> {
    env::var_os(name).map(PathBuf::from).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("Cargo did not provide {name}"),
        )
    })
}

fn collect_assets(root: &Path) -> io::Result<Vec<PathBuf>> {
    let mut pending = vec![root.to_owned()];
    let mut assets = Vec::new();
    let mut total_bytes = 0_u64;
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(&directory)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_symlink() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "UI asset cannot be a symbolic link: {}",
                        entry.path().display()
                    ),
                ));
            }
            if file_type.is_dir() {
                pending.push(entry.path());
                continue;
            }
            if !file_type.is_file() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("UI asset is not a regular file: {}", entry.path().display()),
                ));
            }
            if assets.len() == MAXIMUM_ASSET_FILES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("UI asset count exceeds {MAXIMUM_ASSET_FILES}"),
                ));
            }
            total_bytes = total_bytes
                .checked_add(entry.metadata()?.len())
                .ok_or_else(|| io::Error::other("UI asset byte count overflowed"))?;
            if total_bytes > MAXIMUM_ASSET_BYTES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("UI assets exceed {MAXIMUM_ASSET_BYTES} bytes"),
                ));
            }
            assets.push(
                entry
                    .path()
                    .strip_prefix(root)
                    .map_err(|_| io::Error::other("UI asset escaped its root"))?
                    .to_owned(),
            );
        }
    }
    assets.sort_unstable();
    Ok(assets)
}

fn slash_separated(path: &Path) -> io::Result<String> {
    let mut result = String::new();
    for component in path.components() {
        let Component::Normal(value) = component else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("UI asset path is not relative: {}", path.display()),
            ));
        };
        let value = value.to_str().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("UI asset path is not UTF-8: {}", path.display()),
            )
        })?;
        if !result.is_empty() {
            result.push('/');
        }
        result.push_str(value);
    }
    Ok(result)
}
