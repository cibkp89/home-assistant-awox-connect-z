# Mesh control

Individual mesh control extends the existing command pipeline. The implementation requires known target MAC addresses and
individual mesh IDs. It does not discover unknown lamp identities or provision
new lamps into a radio mesh.

## Components

| File in `mesh/` | Responsibility |
|---|---|
| `__init__.py` | Wire and attach/detach a credential-scoped runtime |
| `models.py` | Directed routes, session proof and learning result types |
| `state.py` | Route facts, learning results, gateway rules, measurements and history |
| `storage.py` | Validate and persist network facts, without session state |
| `routing.py` | Allowed candidates and target mesh path status |
| `gateway.py` | Shared preparation, target leases, exclusive learning, measurements and cancellation |
| `dispatch.py` | Target-specific adapter to the existing command executor |
| `learning.py` | Status-only probes and temporary run progress |

`client.py` still owns BLE connections, authentication, Notify/0x01, encryption
and technical writes. `status_1911.py` owns target/session response storage.
The existing `command/` modules retain sequencing, finalizers, fade spacing,
three/seven-poll confirmation and one complete command-sequence repeat. `availability.py` combines
direct and mesh reachability; entities do not independently invent path rules.

## Network and identity boundaries

Entries with identical mesh name/password share a runtime. Different credentials
produce different storage/runtime keys. Credentials and session keys are not
included in route storage or mesh diagnostics. A MAC may only be registered once
in the runtime; duplicate mesh destinations in that network are rejected.

A route records both endpoint MACs and mesh IDs. It is directed: A → B does not
prove B → A. Removed/unloaded clients cannot serve a route. Changed mesh IDs
invalidate affected routes. An enabled target may only use an enabled gateway
from the same runtime. Mesh availability requires current direct gateway evidence;
an explicit connection attempt may bootstrap a known gateway whose evidence has
expired. It must establish a real direct BLE session, never recursively depend
on another mesh route.

## Scope

Native AwoX area/group entities, group-address commands and membership writes
are not supported. Each lamp retains its individual entity and target-specific
confirmation. Multiple individual lamps may use the same gateway concurrently.
In this document, a command group means a sequence for one target lamp, not a
native AwoX lamp group.

## Learning

Configure opens separate **Runtime settings** and **Mesh management** menus.
Mesh management provides an overview, learning, per-light gateway rules, a directed
matrix and BLE connection measurements. The entry's mesh enable/disable switch
is on the Runtime settings page, alongside its existing operating settings.

**Learn mesh routes** selects existing target lamps of the current entry, in
configured order with all enabled targets initially selected. Gateways can also
belong to other entries with identical mesh credentials.
Only one learning run can own that network at a time. A native progress
page includes the current activity in the progress translation itself, as required
by the native dialog. Opening learning while a run is still active immediately
shows the already-running error; submitting cannot replace that run. The progress
page shows the round, completed checks and current gateway/target; completion opens a
result menu with a per-target summary and one Back action. Poll counts, durations
and individual failure details remain available in mesh diagnostics. Closing
the initiating dialog or explicitly cancelling stops the run and releases its
gateway reservation. Closing a second dialog that merely observes progress does
not cancel someone else's run. Unloading a participating entry also cancels it.
Each dialog registers a shielded, dialog-owned progress waiter with HA, rather
than registering the shared learning task itself: HA cancels its progress task
before removing a flow. Explicitly opening an active run rebinds the observer
to that run without inheriting ownership from an earlier run.
Completed results survive cancellation; a propagated storage error is shown
separately from the in-memory results. HA itself logs some backend write errors
without propagating them to the integration, so those still require checking the
HA log. A later run cannot overwrite an earlier dialog's result.

The run checks selected targets against enabled, configured gateways with current
direct reachability, excluding gateway rules that block that direction.
Five fixed rounds are performed. Each round exclusively reserves a gateway,
disconnects any existing session, connects/authenticates and activates 1911 once,
then checks its selected targets with up to seven directed queries per target
and 250 ms windows. The session is disconnected before releasing the reservation.
Other gateways can continue control. Each target query has a fresh receive cutoff.
Learning never sends Power, Brightness, Color, CCT or Effect commands.

A successful probe permits a future control attempt, which must receive its own
confirmation. Results aggregate the current run, showing successes/attempts such
as 4/5. One or more successful checks retain the direction; zero successes revoke
it. An isolated failed sample cannot erase a success from the same run. On cancel,
only completed samples are retained; their denominator may be less than five.
Unattempted directions retain their previous evidence. Learning does
not update Last mesh gateway or create Mesh connected. Disabling Mesh preserves
stored routes but excludes both the entry's targets and gateways from use.
Its stored gateway rules are inactive while mesh is disabled; ordinary direct
control remains available. Re-enabling mesh restores the saved rules.

The overview separates direct BLE evidence from mesh evidence. The matrix shows
successful checks, missing target replies, technical errors and untested pairs.
Its diagonal shows BLE only after an actual connection; BLE is not a mesh proof.
An asterisk marks historical success not confirmed again in the latest attempt.
Historical evidence never re-enables a failed mesh route. Gateway exclusions are
edited on the gateway settings page; they do not overwrite matrix evidence.
Saved learning evidence does not guarantee present reachability.

## Gateway rules and measurements

Each target can prefer one known gateway and block configured gateways. A direct
path is represented by the target itself and can also be blocked. A preferred
gateway cannot also be blocked. Rules bind both endpoint MACs and mesh IDs;
reused destinations or changed identities do not inherit another lamp's rule.
A preference cannot turn an unlearned indirect path into a candidate.
Only identities referenced by an actual preference or exclusion are stored;
automatic selection without exclusions needs no rule record.

Blocking is checked when preparing a queued request and again at the physical
write/query boundary. A rule change may therefore stop a remaining group; it
never causes a possibly sent group to be replayed through another path.

The BLE measurements page lists configured gateways even before a sample exists.
It records actual runtime `connect()` attempts, successful/failed/cancelled
outcomes and the last available direct RSSI. The displayed duration is the median
of up to 50 successful connections, excluding connect-gate queueing,
authentication and 1911 setup. Existing connections add no sample. Learning
deliberately creates fresh sessions for five samples; normal control adds no
extra radio operations solely for collecting measurements.

Among otherwise comparable mesh gateways, the last 50 attempts inform automatic
selection once at least five successful or failed attempts remain in that window.
Cancelled attempts remain in diagnostics but count neither towards this minimum
nor towards the success-rate denominator. A success rate of at least 80% is
preferred to insufficient data, then to a lower measured rate. Success rate,
successful-connection median and available RSSI break ties, in that order.
Eligible user preferences remain first. Automatic selection prefers a ready
authenticated 1911 session, then an already reserved suitable control gateway.
Direct gateway evidence and route failures precede the BLE quality comparison.
The last confirmed path remains history only and gives no selection preference.
A direct target wins otherwise equivalent cold choices;
it no longer forces a second cold connection ahead of a suitable ready/shared
gateway. Identity, directed learning and exclusion rules still apply.

Composite preparation counters, including authentication/1911, remain separate
diagnostics; they are not presented as pure BLE times. Reset clears both sets of
measurements and starts a new measurement period while preserving direct evidence,
routes, historical results and gateway rules. Statistics are shared by the mesh
network, including compatible entries. Propagated storage failures leave the
in-memory change active and show a warning.

## Control and concurrency

1. Build the direct/learned identity-bound candidate list and exclude blocked
   gateways. Retain known paths without fresh direct evidence as possible
   connection attempts, as in the old coordinator. This does not make the target
   available: availability still requires a live connection or current evidence.
2. Try an eligible preferred gateway first. Otherwise reuse a ready session or
   an already reserved suitable gateway before opening another cold connection.
   Reserving a gateway is visible to other controls before the first await;
   there is no collection timer or group-detection delay.
3. An eligible explicit preference is attempted first even without fresh radio
   evidence. It still requires the configured identities and an active learned
   route for indirect control. Preparation must actually succeed before writing;
   a failed preferred path can use the one allowed distinct alternative.
4. Bind the entire group, polls and repeat to one gateway/session and one target.
   Different targets can share that gateway concurrently. The physical client
   serializes individual GATT operations; target-specific sequences, fade spacing
   and reply waits proceed independently. Requests for the same target remain
   serialized, even across different gateways. Learning takes exclusive access
   on its gateway, with queued probes protected from starvation by newer controls.
   A control waiting for the same target acquires no gateway reader slot until
   that target is free, avoiding needless blocking of unrelated learning work.
5. Prepare connection/authentication/1911 once for all current waiters before
   selecting their lamp commands. A cancelled waiter does not cancel preparation
   needed by another target; the last waiter cancels and awaits unused preparation.
   The existing client bounds the shared work with its normal connect, queue,
   authentication and operation budgets. A
   preparation failure may try at most one distinct alternative, never the same
   candidate twice. Untried alternatives are re-evaluated after preparation
   failure. No third path is attempted for that group.
6. Once prepared, hold that exact gateway/session. Recheck ownership at the
   physical write boundary and for queries/repeats. After any possible write,
   uncertainty or missing confirmation never triggers an automatic route switch.
7. Release the reservation and idle-disconnect pin on completion, error or cancellation.

Different gateways and different targets on one gateway may operate independently.
1911 stores retain separate MAC, destination and session checks for every target;
one target's reply cannot confirm another target's group. Confirmation waits do
not hold the physical write lock, so a delayed or missing reply does not prevent
another target from receiving its command. No broadcast command or physical
simultaneity guarantee is introduced.

The existing shared new-connection
gate and physical command concurrency limits still apply. Waiting for a gateway
lease does not consume an individual connect attempt's timeout. One cancelled
queued user cannot disconnect the gateway currently serving another user.
Physical writes acquire their client's lock before a shared command slot. A
second operation waiting for the same gateway therefore leaves capacity free
for another gateway. Commands, status queries, repeats and initial preparation
use the same lock order; cancellation releases both reservations.

## Confirmation and status

Each target response store belongs to one physical gateway and authentication
generation. Direct stores require outer type `0x80`; forwarded stores require
`0x82`. Both require the configured target mesh ID and reversed MAC suffix.
Each notification is decrypted/decoded once, then independently checked by each
receiving target store. Old callbacks, foreign targets and old buffered responses cannot cross that
boundary. UNKNOWN-state bootstrap uses the same target-specific query path.

The successful selected group reply updates the planner and, when no accepted
follow-up work remains, the HA light state. Other replies do not synchronize the
target's display. Confirmed mesh controls update Last mesh gateway (hexadecimal
gateway Mesh-ID) and establish Mesh connected for that still-active session.

On gateway disconnect/session change the target falls back to Mesh available
if a learned route still has a usable gateway. Otherwise direct evidence decides,
or the target becomes unreachable. No additional target-expiry timer or periodic
mesh polling is introduced. A failed group clears its control proof, but does
not erase the learned route: a later user request may try it again.

Last Seen, Last Bluetooth Source, RSSI and Last Connection retain their direct
meanings. Mesh traffic never fabricates these fields. A lamp never seen by an
adapter can have all four unknown while being controllable through Mesh.

## Persistence and limitations

HA storage persists routes, per-direction learning results, identity-bound gateway
rules, connection measurements and Last mesh gateway. Authentication generations,
keys, response buffers, active proofs and pending requests are never persisted.
Reload cancels users of removed endpoints, releases subscriptions and clears
their session proofs. Saved routes alone do not establish current availability;
a gateway needs fresh/current-process direct evidence after HA restart.
Gateway and target registrations leave the admission registry before asynchronous
unload cleanup starts. New reservations are rejected both before joining a queue
and after acquiring its lock. This prevents late users from escaping the cleanup
snapshot while older users are still releasing the same gateway.
Every direct or forwarded command/query/repeat also checks that both registered
client objects and their configured destination IDs still belong to the bound
route. A gateway removed before its preparation task starts cannot connect.

1911 contains no unique query nonce or trustworthy device measurement timestamp.
Session/identity/receive-time checks cannot establish strict causal attribution
for a delayed or unsolicited reply from the same target in the same session.
Neither a matching status nor Mesh connected proves physical light output.
Targeted two-lamp ESPHome/BLE checks cover shared cold/warm control and error
handling, with HA framework/storage/discovery scaffolding. They included an
unconfirmed mesh OFF and a shared connection failure; radio reliability is not
guaranteed. The final attempt-selection rule was additionally checked after real
expiry of gateway evidence. Subsequent Home Assistant acceptance covered
direct and mesh control, shared cold/warm switching, combinations, rapid changes,
reload and recovery after power loss. The user confirmed physical light output
for the observed checks and separately checked the remaining connection-error
case. This is two-lamp coverage, not a guarantee for other firmware or larger
meshes. Offline tests simulate BLE transport.

Storage/publication listener errors are logged and isolated. They do not turn a
confirmed command into a failed command or prevent other listeners from running.
Confirmed control history retains an unreachable state when no usable route remains,
even after an explicit failed revalidation removes the last route.
