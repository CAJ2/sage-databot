---
glob: '**/*.ts'
---

# TypeScript Scripts

## TypeScript (Bun) — Default

Bun runtime with full npm ecosystem and fastest execution.

### Structure

Export a single **async** function called `main`:

```typescript
export async function main(param1: string, param2: number) {
  // Your code here
  return { result: param1, count: param2 }
}
```

Do not call the main function. Libraries are installed automatically.

### Resource Types

Use the `RT` namespace for resource types:

```typescript
export async function main(stripe: RT.Stripe) {
  // stripe contains API key and config from the resource
}
```

Only use resource types if needed. Always use the RT namespace.

### Imports

```typescript
import Stripe from 'stripe'
import { someFunction } from 'some-package'
```

### Windmill Client

```typescript
import * as wmill from 'windmill-client'
```

### Preprocessor Scripts

```typescript
type Event = {
  kind:
    | 'webhook'
    | 'http'
    | 'websocket'
    | 'kafka'
    | 'email'
    | 'nats'
    | 'postgres'
    | 'sqs'
    | 'mqtt'
    | 'gcp'
  body: any
  headers: Record<string, string>
  query: Record<string, string>
}

export async function preprocessor(event: Event) {
  return {
    param1: event.body.field1,
    param2: event.query.id,
  }
}
```

### S3 Object Operations

```typescript
type S3Object = {
  s3: string // Path within the bucket
}

import * as wmill from 'windmill-client'

// Load file content from S3
const content: Uint8Array = await wmill.loadS3File(s3object)

// Load file as stream
const blob: Blob = await wmill.loadS3FileStream(s3object)

// Write file to S3
const result: S3Object = await wmill.writeS3File(
  s3object, // Target path (or undefined to auto-generate)
  fileContent, // string or Blob
  s3ResourcePath, // Optional: specific S3 resource to use
)
```

---

## TypeScript (Bun Native)

Native TypeScript execution with fetch only — no external imports allowed.

### Structure

Same `main` export pattern. No imports allowed. Use the globally available `fetch`:

```typescript
export async function main(url: string) {
  const response = await fetch(url)
  return await response.json()
}
```

The windmill client is not available in native TypeScript mode.

---

## TypeScript (Deno)

Deno runtime with npm support via `npm:` prefix and native Deno libraries.

### Imports

```typescript
// npm packages use npm: prefix
import Stripe from 'npm:stripe'
import { someFunction } from 'npm:some-package'

// Deno standard library
import { serve } from 'https://deno.land/std/http/server.ts'
```

### Windmill Client

```typescript
import * as wmill from 'windmill-client'
```

---

## TypeScript (Native)

Same as Bun Native — fetch only, no imports, windmill client unavailable.
