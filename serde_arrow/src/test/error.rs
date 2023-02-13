use crate::internal::error::error;

#[test]
fn error_can_be_converted_to_anyhow() {
    fn _func() -> anyhow::Result<()> {
        Err(error!("dummy"))?;
        Ok(())
    }
}
