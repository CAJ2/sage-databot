import { createAppAuth } from '@octokit/auth-app'
import { Octokit } from '@octokit/rest'
import * as wmill from 'windmill-client'

const COMMENT_MARKER = '<!-- windmill-integration-tests -->'

async function getOctokit(): Promise<Octokit> {
  const raw = await wmill.getVariable('f/api_config/github_app')
  const { app_id, private_key, installation_id } = JSON.parse(raw)

  const auth = createAppAuth({
    appId: Number(app_id),
    privateKey: private_key,
    installationId: Number(installation_id),
  })
  const { token } = await auth({ type: 'installation' })

  return new Octokit({ auth: token })
}

async function createCheckRun(repo: string, headSha: string): Promise<number> {
  const octokit = await getOctokit()
  const [owner, repoName] = repo.split('/')
  const { data } = await octokit.checks.create({
    owner,
    repo: repoName,
    name: 'Integration Tests',
    head_sha: headSha,
    status: 'in_progress',
    started_at: new Date().toISOString(),
  })
  return data.id
}

async function updateCheckRun(
  repo: string,
  checkRunId: number,
  success: boolean,
  summary: string,
  text: string,
): Promise<void> {
  const octokit = await getOctokit()
  const [owner, repoName] = repo.split('/')
  await octokit.checks.update({
    owner,
    repo: repoName,
    check_run_id: checkRunId,
    status: 'completed',
    conclusion: success ? 'success' : 'failure',
    completed_at: new Date().toISOString(),
    output: {
      title: `Integration Tests — ${summary}`,
      summary,
      text,
    },
  })
}

async function postStickyComment(repo: string, prNumber: number, body: string): Promise<void> {
  const octokit = await getOctokit()
  const [owner, repoName] = repo.split('/')
  const markedBody = `${COMMENT_MARKER}\n${body}`

  let existingId: number | undefined
  for await (const { data: comments } of octokit.paginate.iterator(octokit.issues.listComments, {
    owner,
    repo: repoName,
    issue_number: prNumber,
    per_page: 100,
  })) {
    const found = comments.find((c: { id: number; body?: string }) =>
      c.body?.includes(COMMENT_MARKER),
    )
    if (found) {
      existingId = found.id
      break
    }
  }

  if (existingId !== undefined) {
    await octokit.issues.updateComment({
      owner,
      repo: repoName,
      comment_id: existingId,
      body: markedBody,
    })
  } else {
    await octokit.issues.createComment({
      owner,
      repo: repoName,
      issue_number: prNumber,
      body: markedBody,
    })
  }
}

/**
 * Called twice by the run_tests flow:
 *
 * 1. Create pending check run (pass head_sha, omit check_run_id/results):
 *    Returns { check_run_id }
 *
 * 2. Report final results (pass check_run_id + results, omit head_sha):
 *    Updates check run, posts sticky PR comment if pr_number provided.
 *    Returns { success, summary }
 */
export async function main(
  repo: string,
  head_sha: string | null = null,
  check_run_id: number | null = null,
  results: { success: boolean; summary: string; markdown: string } | null = null,
  pr_number: number | null = null,
): Promise<{ check_run_id: number } | { success: boolean; summary: string }> {
  if (head_sha !== null) {
    const id = await createCheckRun(repo, head_sha)
    console.log(`Created check run ${id} for ${repo}@${head_sha.slice(0, 8)}`)
    return { check_run_id: id }
  }

  if (check_run_id !== null && results !== null) {
    const { success, summary, markdown } = results
    await updateCheckRun(repo, check_run_id, success, summary, markdown)
    if (pr_number !== null) {
      await postStickyComment(repo, pr_number, markdown)
    }
    console.log(`Updated check run ${check_run_id}: ${success ? 'success' : 'failure'}`)
    return { success, summary }
  }

  throw new Error('Provide either head_sha (create mode) or check_run_id + results (report mode)')
}
