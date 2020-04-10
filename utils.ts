import * as Queue from 'bee-queue'
import { CronJob } from 'cron'
import * as fs from 'fs'
import _bugsnag, { Bugsnag } from '@bugsnag/js'
import * as moment from 'moment'

export function delayed(delayInMilliseconds: number, aggregateAttributes: string[], attributesFn: (job: Queue.Job) => Promise<{ [key: string]: string | number }>) {
  return async function (job: Queue.Job) {
    const { name } = job.data
    const attributes = await attributesFn(job)
    const real_name = name.replace(/_delayed$/, '')
    // if attributesFn removes id from job.data
    const aggregated = {}
    function aggregate(job: Queue.Job) {
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
      jobs = jobs.filter(j => (j.data.name === real_name && Object.keys(attributes).every(key => j.data[key] == attributes[key])))

      for (const j of jobs) {
        aggregate(job)
        removedJobs.push(j.id)
        await j.remove()
      }
    }
    aggregate(job)

    const newJobData = {
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

export function processAll(name: string, options: { directory: string, beforeStart?: () => Promise<void> }) {
  const { directory: dir, beforeStart } = options
  if (!fs.existsSync(dir)) {
    return
  }

  let bugsnag: Bugsnag.Client
  if (process.env.BUGSNAG_API_KEY) {
    bugsnag = _bugsnag({
      apiKey: process.env.BUGSNAG_API_KEY,
      appType: `worker:${name}`
    })
  } else {
    bugsnag = { notify: console.log } as Bugsnag.Client
  }

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
    const ext = __filename.substring(__filename.lastIndexOf('.'))
    
    if (!file.startsWith('index') && file.endsWith(ext)) {
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

  queue.process(2, async (job: Queue.Job) => {
    !job.data.delay && console.log('start')
    if (beforeStart) {
      await beforeStart()
    }
    const { name, delay } = job.data
    if (name) {
      const module = modules[name]
      if (module) {
        return await module.default(job)
      } else {
        if (name.endsWith('_delayed')) {
          return await delayed(delay || 12e4, [], job => {
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

  queue.on('succeeded', (job: Queue.Job, result: any) => {
    !job.data.delay && console.log(JSON.stringify({ status: 'success', job_id: job.id, job_name: job.data.name, id: job.data.id, result }))
    job.remove()
  })

  queue.on('retrying', (job: Queue.Job, err: Error) => {
    console.error({ error: err, job_id: job.id, job_name: job.data.name, id: job.data.id })
  })

  queue.on('failed', (job: Queue.Job, err: Error) => {
    console.error({ error: err, job_id: job.id, job_name: job.data.name, id: job.data.id })
    bugsnag.notify(err, { metaData: { req: job.data } })
    job.remove()
  })

  queue.on('stalled', (job_id: string) => {
    console.log(JSON.stringify({ status: 'stalled', job_id }))
  })

  return queue
}