/**
 * Content-history contract (2026-10-12 收窄版).
 *
 * Scope after the trim (docs/memory-design-proposal.md §4.8 / §5):
 *   - KEEP: full versions for L1/L2/L3 with Core-recorded time and sequence;
 *     read by version or "as of" a recorded time; history and change listing;
 *     CAS baseline checks and operation idempotency; L1 source message ids;
 *     L2/L3 generation input lists (recorded in metadata / sidecar).
 *   - REMOVED: recognition blocks, typed relations (supports/premise/conflicts
 *     and their forward/reverse/multi-hop queries), the two-version diff
 *     interface, event/validity time classification, change explanations,
 *     and content identity continuation across rename/merge/split.
 *     Relationships and differences are derived by the model at read time
 *     from the stored versions, never pre-built by the program.
 *
 * Storage implementations (SQLite for L0/L1, local snapshot for L2/L3) and the
 * gateway both implement/validate against this module.
 */
export type HistoryLayer = 'l0' | 'l1' | 'l2' | 'l3';
export interface HistoryScope {
  team_id?: string; user_id?: string; agent_id?: string; task_id?: string; session_id?: string;
}
export interface VersionRef { layer: HistoryLayer; content_id: string; version_id: string; }
/**
 * Change kinds kept after the trim. `rename`/`merge`/`split` are gone: a
 * locator IS the identity's address, so a "rename" is simply a new identity at
 * the new locator plus (optionally) a retire at the old one.
 */
export type ChangeKind = 'baseline' | 'create' | 'revise' | 'retire' | 'restore';
export interface VersionDraft {
  layer: HistoryLayer;
  /** Absent on new content; program allocates. Not a filesystem path. */
  content_id?: string;
  /** Current underlying record id or profile path, not the stable identity. */
  locator: string;
  scope: HistoryScope;
  body: string;
  /** JSON-safe complete content metadata, including original sources and conditions. */
  metadata: Record<string, unknown>;
  state: 'active' | 'retired';
}
export interface ContentVersion extends VersionDraft {
  content_id: string;
  ref: VersionRef;
  recorded_at: string;
  sequence: number;
  predecessors: VersionRef[];
  change_kind: ChangeKind;
  history_complete: boolean;
}
export interface VersionMutation {
  draft: VersionDraft;
  /** null requires a new identity; exact version id for an existing identity. */
  expected_version_id: string | null;
  /** Exact old versions (empty or the single prior version after the trim). */
  predecessors: VersionRef[];
  change_kind: ChangeKind;
}
export interface ChangeCommit {
  /** Reusing with different payload is a conflict, same payload returns same result. */
  operation_id: string;
  actor: 'user' | 'model' | 'program';
  mutations: VersionMutation[];
}
export interface ChangeCommitResult {
  changed: boolean;
  versions: ContentVersion[];
  sequence: number;
}
export interface HistoryRead {
  layer: HistoryLayer; content_id: string; scope: HistoryScope;
  version_id?: string;
  /** Inclusive Core-recorded time, never event time. Mutually exclusive with version_id. */
  as_of?: string;
  /** Pin this adapter's successful commit boundary. */
  upper_sequence?: number;
}
export interface HistoryPageQuery {
  scope: HistoryScope; layer?: HistoryLayer; content_id?: string;
  from_recorded_at?: string; to_recorded_at?: string;
  upper_sequence?: number;
  /** Ascending keyset pagination; sequence alone cannot disambiguate multi-item commits. */
  after?: { sequence: number; version_id: string };
  limit: number;
}
export interface HistoryPage {
  items: ContentVersion[];
  upper_sequence: number;
  next: { sequence: number; version_id: string } | null;
  history_complete: boolean;
}
/** Sync SQLite and async filesystem are both allowed. Router awaits either. */
export interface HistoryReader {
  readVersion(query: HistoryRead): ContentVersion | null | Promise<ContentVersion | null>;
  listChanges(query: HistoryPageQuery): HistoryPage | Promise<HistoryPage>;
  /** Returns the exact version associated with an old locator, NOT its current successor. */
  resolveLocator(layer: HistoryLayer, locator: string, scope: HistoryScope): VersionRef | null | Promise<VersionRef | null>;
}
