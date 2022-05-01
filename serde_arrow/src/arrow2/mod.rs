mod to_chunk;

#[cfg(feature = "arrow2-io_ipc")]
mod write_ipc;

pub use to_chunk::to_chunk;

#[cfg(feature = "arrow2-io_ipc")]
pub use write_ipc::write_ipc;
