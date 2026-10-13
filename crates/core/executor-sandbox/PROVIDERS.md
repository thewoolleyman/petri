# Registered sandbox providers

A sandbox-driver plugin Petri does not know about, say `lima`, can be
registered under its own provider kind and selected like a built-in one. Its
ordinary process scopes run on it, and resume, release, and prune go back to
it by each lease's record. Registering launches nothing: the plugin starts on
the first scope that needs it.

## CLI

The same provider file serves `run`, `resume`, and `sandbox prune`:

```sh
petri run workflow.petri.yaml --backend lima --sandbox-providers providers.toml
petri resume --run-dir ./run --sandbox-providers providers.toml
petri sandbox prune --run-dir ./run --sandbox-providers providers.toml
```

```toml
[providers.lima]
executable = "./sandbox-driver-lima"   # relative to this file
args = ["--socket", "/tmp/lima.sock"]
sha256 = "…"                           # the executable's SHA-256, or `dev = true`

[providers.lima.env]                   # the plugin's whole environment
PATH = "/usr/bin:/bin"

[providers.lima.process_spec]          # sandbox-driver's SandboxSpec, as TOML
source = "host_directory"
```

- `executable` is resolved against the file's directory and must exist when
  the plugin launches. Nothing is looked for on `PATH`.
- `sha256` pins the executable; `dev = true` launches it unverified. One of
  the two is required.
- `env` is the plugin's complete environment. Nothing is inherited from
  Petri's own.
- `process_spec` is the template every process scope is built from, in
  sandbox-driver's `SandboxSpec` schema: its source, resources, working
  directory, network policy, timers, volumes, and opaque `provider_config`
  pass through as given. Petri sets the sandbox name and the `petri.*` labels
  and lays the scope's environment over the template's. A field the schema
  does not have is an error, not ignored.

A parse error reports a location only, since the file can hold credentials.
Nothing in it reaches the workflow graph or the run's records, so a resume
or prune is given the file again.

A Fabro `workflow.toml` selects the provider by name:
`[environments.<id>] provider = "lima"`. The name is carried through lowering
as written. `image` and `resources` are refused under a registered provider,
since its sandbox comes from the registration; environment variables apply
as they do everywhere.

## Embedding

```rust,ignore
let mut providers = ProviderRegistrations::new();
providers.register_linked(host_factory)?;            // built-in kinds only
providers.register_plugin(lima_config, lima_spec)?;  // custom kinds only
let runtime = Runtime::standard().sandbox_providers(providers);
```

`ProviderRegistrations::standalone()` is what the CLI starts from: the Host,
Docker, and Daytona plugins configured from the environment on first use.
`register_plugin` takes sandbox-driver's `PluginConfig` with an absolute
executable, its arguments, its complete environment, and a checksum or
`dev`, plus the process template. Each kind registers once; a kind with no
entry fails at acquire, and no plugin is launched in its place.

## What a registered provider serves

Ordinary process scopes only. A container target, services, placement
requirements (a GitHub Actions `runs-on`), and Docker actions need a recipe
Petri has only for its built-in kinds, and are refused before the plugin is
reached. The sandbox has no route back to Petri's machine and mounts no host
workspace.

The run's network policy, when it is not the provider default, must equal
the template's; when either side is the default, the other applies. A run
asking for `block` is verified against the sandbox's reported policy, as it
is for the built-in providers.

## Identity and recovery

The plugin's health report must name a stable, non-secret identity for the
resource namespace it serves (a host, an account, a VM endpoint). The
fingerprint recorded on every lease is `<kind>:<identity>`; a plugin that
reports none cannot create or recover anything. Credentials and the
executable's location do not enter it, so they can change without
orphaning a run's sandboxes.

Resume, release, and prune compare each lease's recorded fingerprint with the
provider's before touching it, and reconcile verifies every recorded lease
before adopting or sweeping anything on a provider. A mismatch refuses the
recorded leases; a new scope on the mismatched provider still creates there,
under the provider's current fingerprint, so a run can end up with leases in
two namespaces until the configuration is restored.

The reference plugin, `fixture-provider` in the test kit, is test
scaffolding over the Host provider, not an isolation boundary.
