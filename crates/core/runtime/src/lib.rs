//! Core, assembled.
//!
//! Every other core crate is one layer and names no other implementation: the
//! IR knows no executor, the executor interface knows no backend, the frontend
//! trait knows no format. This crate is the one place those pieces are wired
//! together — [`Runtime`] holds a frontend list, a step registry, an executor
//! per [`ir::RuntimeTarget`], secrets and options, and drives a graph with the
//! replay canary on. It is also the facade: [`ir`], [`engine`], [`driver`],
//! [`frontend`], [`steps`] and [`executor`] are reachable through it, so one
//! dependency reaches all of core.
//!
//! The wiring goes one way. A component — another workflow format, another step
//! kind — *registers onto* this builder with [`Runtime::frontend`] and
//! [`Runtime::step`]; nothing registers *into* it, and no crate under `core`
//! names a component. The distribution that ships is assembled a layer up, in
//! `petri`, which is the crate that names them all.

pub use driver;
pub use engine;
pub use ir;
pub use store;

/// The executor interface, with the sandbox-driver adapter — the executors the
/// standard runtime uses — as a submodule.
pub mod executor {
    pub use ::executor::*;
    pub use executor_sandbox as sandbox;
}

/// The frontend interface, with the native format as a submodule.
pub mod frontend {
    pub use ::frontend::*;
    pub use frontend_native as native;
}

/// Step kinds, with the standard registry.
pub mod steps {
    pub use ::steps::*;

    /// The built-in step kinds: `noop` and `process`.
    pub fn standard() -> Registry {
        let mut registry = Registry::new();
        registry.register(::steps::NoopStep);
        registry.register(::steps::ProcessStep);
        registry
    }
}

mod runtime;

pub use executor_sandbox::{
    DaytonaResources, DaytonaSandboxKind, InProcessProviders, LostSandbox, ProviderContext,
    ProviderFactory, ProviderNetwork, ProviderRegistrations, RegistrationError, SandboxBackend,
    SandboxOptions, fingerprint,
};
pub use runtime::{
    AdmissionPass, AdmissionProblem, LoadError, RunAccess, RunOptions, RunRuntime, RunServiceGuard,
    Runtime,
};
