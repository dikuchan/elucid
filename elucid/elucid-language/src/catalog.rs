use elucid_catalog::Source;

#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct CatalogSnapshot<'catalog> {
    source: Option<&'catalog Source>,
}

impl<'catalog> CatalogSnapshot<'catalog> {
    #[must_use]
    pub const fn new(source: &'catalog Source) -> Self {
        Self {
            source: Some(source),
        }
    }

    #[must_use]
    pub const fn empty() -> Self {
        Self { source: None }
    }

    #[must_use]
    pub fn source(&self, name: &str) -> Option<&'catalog Source> {
        self.source.filter(|source| source.name().as_str() == name)
    }
}
