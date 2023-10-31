use std::sync::RwLock;

pub static CONFIGURATION: RwLock<Configuration> = RwLock::new(Configuration {
    debug_print_program: false,
    _prevent_construction: (),
});

/// The crate settings can be configured by calling [configure]
#[derive(Default, Clone)]
pub struct Configuration {
    pub(crate) debug_print_program: bool,
    /// A non public member to allow extending the member list as non-breaking
    /// changes
    _prevent_construction: (),
}

/// Change global configuration options
///
/// Note the configuration will be shared by all threads in the current program.
/// Thread-local configurations are not supported at the moment.
///
/// Usage:
///
/// ```ignore
/// serde_arrow::experimental::configure(|c| {
///     // set attributes on c
/// });
/// ```
#[allow(unused)]
pub fn configure<F: FnOnce(&mut Configuration)>(f: F) {
    let mut guard = CONFIGURATION.write().unwrap();
    f(&mut guard)
}
