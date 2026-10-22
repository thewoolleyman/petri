//! A real protocol peer of an unfamiliar kind over a durable local backend:
//! the Host provider under a registry of its own, serving sandbox-driver's
//! stdio protocol as any plugin does. It records every call it serves and
//! the spec it was asked to create, reports the identity `FIXTURE_IDENTITY`
//! names, and can crash once after a create to lose the reply. Its controls
//! are explicit arguments and environment; stdout belongs to the protocol.

use std::fs::{self, OpenOptions};
use std::io::Write as _;
use std::path::PathBuf;
use std::sync::Arc;
use std::{env, process};

use async_trait::async_trait;
use sandbox_driver::{
    Capabilities, EventContext, HealthStatus, ProviderHealth, ProviderKind, Sandbox, SandboxFilter,
    SandboxId, SandboxProvider, SandboxSpec, SandboxStatus,
};
use sandbox_driver_host::HostProvider;

struct Fixture {
    kind:  ProviderKind,
    inner: HostProvider,
    root:  PathBuf,
}

impl Fixture {
    fn record(&self, operation: &str) {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.root.join("calls"))
            .expect("audit file");
        writeln!(file, "{operation}").expect("audit operation");
    }
}

#[async_trait]
impl SandboxProvider for Fixture {
    fn kind(&self) -> &ProviderKind {
        &self.kind
    }
    fn capabilities(&self) -> &Capabilities {
        self.inner.capabilities()
    }
    async fn create(
        &self,
        spec: &SandboxSpec,
        events: Option<EventContext>,
    ) -> sandbox_driver::Result<Arc<dyn Sandbox>> {
        self.record("create");
        fs::write(
            self.root.join("spec.json"),
            serde_json::to_vec(spec).expect("spec JSON"),
        )
        .expect("captured spec");
        // The fixture accepts an opaque option to check Petri's forwarding;
        // the directory backend does not itself understand that option.
        let mut host_spec = spec.clone();
        host_spec.provider_config = serde_json::Value::Null;
        let sandbox = self.inner.create(&host_spec, events).await?;
        if self.root.join("crash-after-create").exists() {
            fs::remove_file(self.root.join("crash-after-create")).expect("consume crash marker");
            crash();
        }
        Ok(sandbox)
    }
    async fn attach(
        &self,
        id: &SandboxId,
        events: Option<EventContext>,
    ) -> sandbox_driver::Result<Arc<dyn Sandbox>> {
        self.record("attach");
        self.inner.attach(id, events).await
    }
    async fn delete(
        &self,
        id: &SandboxId,
        events: Option<EventContext>,
    ) -> sandbox_driver::Result<()> {
        self.record("delete");
        self.inner.delete(id, events).await
    }
    async fn list(&self, filter: &SandboxFilter) -> sandbox_driver::Result<Vec<SandboxStatus>> {
        self.record("list");
        self.inner.list(filter).await
    }
    async fn health(&self) -> sandbox_driver::Result<ProviderHealth> {
        self.record("health");
        let mut report = ProviderHealth::new(HealthStatus::Ok);
        report.identity = env::var("FIXTURE_IDENTITY").ok();
        Ok(report)
    }
}

#[expect(
    clippy::exit,
    reason = "the fixture deliberately loses a mutation reply to test recovery"
)]
fn crash() -> ! {
    process::exit(73)
}

#[tokio::main]
async fn main() {
    assert_eq!(
        env::args().nth(1).as_deref(),
        Some("--fixture"),
        "explicit arguments must reach the plugin"
    );
    let root = PathBuf::from(env::var_os("FIXTURE_ROOT").expect("explicit fixture directory"));
    fs::create_dir_all(&root).expect("fixture directory");
    let provider = Fixture {
        kind: ProviderKind::try_new(
            env::var("FIXTURE_KIND").unwrap_or_else(|_| "fixture-provider".to_owned()),
        )
        .expect("fixture kind"),
        inner: HostProvider::with_registry(root.join("registry"))
            .await
            .expect("durable registry"),
        root,
    };
    provider.record("launch");
    sandbox_driver_protocol::serve_stdio(Arc::new(provider))
        .await
        .expect("serve fixture");
}
