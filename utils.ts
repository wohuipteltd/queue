import * as Queue from 'bee-queue'
import { CronJob } from 'cron'
import * as fs from 'fs'
import Bugsnag, { Client } from '@bugsnag/js'
import * as moment from 'moment'

type JobOptions = { queue?: string }

export abstract class Queues {
  protected queueByName(name: string): Queue {
    throw new Error('unimplemented')
  }

  public addJob<U extends JobOptions>(name: string, options: U, returnId: true): Promise<Queue.Job<Omit<U, 'queue'> & { name: string }>>
  public addJob<U extends JobOptions>(name: string, options: U, returnId: false): Queue.Job<Omit<U, 'queue'> & { name: string }>
  public addJob<U extends JobOptions>(name: string, options: U): Queue.Job<Omit<U, 'queue'> & { name: string }>

  public addJob<U extends JobOptions>(name: string, options: U, returnId: boolean = false): Promise<Queue.Job<Omit<U, 'queue'> & { name: string }>> | Queue.Job<Omit<U, 'queue'> & { name: string }> {
    const { queue, ...others } = options
    const job = this.queueByName(queue || name).createJob({ name, ...others })
    if (process.env.NODE_ENV === 'test') {
      if (returnId) return Promise.resolve(job)
      return job
    }
    const future = job.save()
    if (returnId) return future
    return job
  }

  public async stubJob<T>(job: Queue.Job<T>, timeout_limit: number = 30_000) {
    const jobPromise = new Promise((resolve, reject) => {
      if (job.status === 'succeeded') {
        return resolve(undefined)
      }
      if (job.status === 'failed') {
        return reject(undefined)
      }
      const timeout = setTimeout(() => {
        resolve(null)
      }, timeout_limit)
      job.on('failed', reject)
      job.on('succeeded', (result) => {
        clearTimeout(timeout)
        resolve(result)
      })
    })
    return await jobPromise
  }

  public async stub(name: string, options: JobOptions, timeout_limit: number = 30_000) {
    const job = await this.addJob(name, options, true)
    return await this.stubJob(job, timeout_limit)
  }
}

function delayed<T extends A & { name: string}, A = { [key: string]: string | number }>(delayInMilliseconds: number, aggregateAttributes: string[], attributesFn: (job: Queue.Job<T>) => Promise<A>) {
  return async function (job: Queue.Job<T>) {
    const { name } = job.data
    const attributes = await attributesFn(job)
    const real_name = name.replace(/_delayed$/, '')
    // if attributesFn removes id from job.data
    const aggregated: any = {}
    function aggregate(job: Queue.Job<T>) {
      for (const attr of aggregateAttributes) {
        const key = `${attr}s`
        aggregated[key] = Array.from(new Set([...(aggregated[key] || []), ...(job.data[key] || []), job.data[attr]]))
      }
    }

    let idx = 0
    const removedJobs = []
    while (idx >= 0) {
      const page: Queue.Page = { start: idx * 100, end: (idx + 1) * 100 }
      let jobs = await job.queue.getJobs('delayed', page)
      if (jobs.length < 100) {
        idx = -1
      } else {
        idx++
      }
      jobs = jobs.filter(j => (j.data.name === real_name && Object.keys(attributes).every(key => JSON.stringify(j.data[key]) == JSON.stringify(attributes[key]))))

      for (const j of jobs) {
        aggregate(job)
        removedJobs.push(j.id)
        await j.remove()
      }
    }
    aggregate(job)

    const newJobData: T = {
      name: real_name,
      ...attributes,
      ...aggregated
    }
    const newJob = await job.queue.createJob(newJobData)
      .delayUntil(Date.now() + delayInMilliseconds)
      .save()
    return { newJobData, newJobId: newJob.id }
  }
}

const __clients: { [name: string]: Client } = {}

const lazyloadBugsnag = (name: string) => {
  if (!__clients[name]) {
    let bugsnag: Client
    if (process.env.BUGSNAG_API_KEY) {
      bugsnag = Bugsnag.start({
        apiKey: process.env.BUGSNAG_API_KEY,
        appType: `worker:${name}`
      })
    } else {
      bugsnag = { notify: console.log } as Client
    }
    __clients[name] = bugsnag
  }
  return __clients[name]
}
export function processAll(name: string, options: { directory: string, beforeStart?: () => Promise<void> }) {
  const { directory: dir, beforeStart } = options
  if (!fs.existsSync(dir)) {
    return
  }

  const bugsnag = lazyloadBugsnag(name)
  const queue = new Queue(name, {
    redis: { url: process.env.REDIS_URL },
    isWorker: true,
    storeJobs: true,
    ensureScripts: true,
    activateDelayedJobs: true,
    removeOnSuccess: true,
    removeOnFailure: true
  })
  const modules = {}

  for (const file of fs.readdirSync(dir)) {
    if (!file.startsWith('index') && (file.endsWith('.ts') || file.endsWith('.js')) ) {
      const module = require(`${dir}/${file}`)
      if (module && typeof module.default === 'function') {
        const name = file.substring(0, file.lastIndexOf('.'))
        modules[name] = module
        if (process.env.CRONJOB === '1' && typeof module.cron === 'string') {
          const cronJob = new CronJob({
            cronTime: module.cron,
            timeZone: 'Asia/Shanghai',
            onTick: () => {
              console.log(`dispatching cron job: ${name}`)
              queue.createJob({ name }).save()
            }
          })
          console.log(`installing cronjob ${name}: ${module.cron} (next ${moment(cronJob.nextDates()).fromNow()})`)
          cronJob.start()
        }
      }
    }
  }

  queue.process(2, async (job: Queue.Job<any>) => {
    !job.data.delay && console.log(JSON.stringify({ status: 'start', time: Date.now(), job_id: job.id, job_name: job.data.name, id: job.data.id }))
    if (beforeStart) {
      await beforeStart()
    }
    const { name, delay } = job.data
    if (name) {
      const module = modules[name]
      if (module) {
        return await module.default(job, bugsnag)
      } else {
        if (name.endsWith('_delayed')) {
          return await delayed(delay || 12e4, [], (job: Queue.Job<any>) => {
            const { name, ...others } = job.data
            return others
          })(job)
        }
      }
    }
    console.error(`processor with name '${name}' not found`)
  })

  queue.on('ready', () => {
    console.log('queue is ready')
  })

  queue.on('error', (err: Error) => {
    console.error(`queue error ${err}`)
    bugsnag.notify(err)
  })

  queue.on('succeeded', (job: Queue.Job<any>, result: any) => {
    !job.data.delay && console.log(JSON.stringify({ status: 'success', time: Date.now(), job_id: job.id, job_name: job.data.name, id: job.data.id, result }))
    job.remove()
  })

  queue.on('retrying', (job: Queue.Job<any>, err: Error) => {
    console.error({ error: err, job_id: job.id, job_name: job.data.name, id: job.data.id })
  })

  queue.on('failed', (job: Queue.Job<any>, err: Error) => {
    console.error({ error: err, job_id: job.id, job_name: job.data.name, id: job.data.id })
    bugsnag.notify(err, event => {
      event.addMetadata('JobData', job.data)
    })
    job.remove()
  })

  queue.on('stalled', (job_id: string) => {
    console.log(JSON.stringify({ status: 'stalled', job_id }))
  })

  return queue
}