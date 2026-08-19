use elucid_catalog::Source;

#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct CatalogSnapshot<'catalog> {
    source: &'catalog Source,
}

impl<'catalog> CatalogSnapshot<'catalog> {
    #[must_use]
    pub const fn new(source: &'catalog Source) -> Self {
        Self { source }
    }

    #[must_use]
    pub fn source(&self, name: &str) -> Option<&'catalog Source> {
        (self.source.name().as_str() == name).then_some(self.source)
    }
}
