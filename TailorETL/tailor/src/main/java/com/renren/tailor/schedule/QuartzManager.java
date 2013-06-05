package com.renren.tailor.schedule;

import java.util.Date;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.CronScheduleBuilder;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.core.jmx.JobDataMapSupport;
import org.quartz.impl.StdSchedulerFactory;

public class QuartzManager {

	private static Log logger = LogFactory.getLog(QuartzManager.class);
	
	private static final SchedulerFactory schedulerFactory = new StdSchedulerFactory();

	private static final MRJobListener mrJobListener = new MRJobListener(
			"myjoblistener");
	private static final MRTriggerListener triggerListener = new MRTriggerListener(
			"mytriggerlistener");
	
	private static final String JOB_GROUP_NAME="TAILOR_JOBGROUP_NAME";

	private static Scheduler schedule = null;
	
	public  static boolean isStart=false;

	
	 public static void startQuartz(){
	   if(!isStart){
	     try {
         schedule=schedulerFactory.getScheduler();
         schedule.start();
         schedule.getListenerManager().addJobListener(mrJobListener);
         schedule.getListenerManager().addTriggerListener(triggerListener);
         isStart=true;
       } catch (SchedulerException e) {
         e.printStackTrace();
       }
	   }
	  }

	public static void shutdownJobs() {
		try {
			if (!schedule.isShutdown()) {
				schedule.shutdown();
				isStart=false;
			}
		} catch (SchedulerException e) {
			e.printStackTrace();
		}
	}

	public static void scheduleCronJob(Class<? extends Job> jobClass,String jobKey,
			 Map<String, Object> maps, String cron, Date startTime) {
		if(null==startTime)startTime=new Date();
		JobDetail job = createJob(jobClass,  jobKey, maps);
		Trigger trigger = null;
		if (cron.matches("[0-9]+")) {
			trigger = createSimpleTrigger(Integer.parseInt(cron), jobKey, startTime);
		} else {
			cron=cron.replace(",", " ");
			trigger = createCronTrigger(cron,  jobKey,startTime);
		}
		try {
			schedule.scheduleJob(job, trigger);
		} catch (SchedulerException e) {
			e.printStackTrace();
		}
	}

	public static void scheduleOnceJob(Class<? extends Job> jobClass,String jobKey,
			 Map<String, Object> maps) {
		JobDetail job = createJob(jobClass,jobKey,  maps);
		Trigger trigger = TriggerBuilder
				.newTrigger()
				.startNow()
				.build();
		try {
			schedule.scheduleJob(job, trigger);
		} catch (SchedulerException e) {
			logger.error("once job error"+e.getMessage());
			e.printStackTrace();
		}
		
		/*
		DirectSchedulerFactory factory = DirectSchedulerFactory.getInstance();
		try {
			factory.createVolatileScheduler(1);
			Scheduler onceSchedule=factory.getScheduler();
			onceSchedule.start();
			onceSchedule.getListenerManager().addJobListener(mrJobListener);
			onceSchedule.getListenerManager().addTriggerListener(triggerListener);
			onceSchedule.scheduleJob(job, trigger);
		} catch (SchedulerException e) {
			e.printStackTrace();
		}
		*/
	}

	public static JobDetail createJob(Class<? extends Job> jobClass,String jobKey,
			 Map<String, Object> maps) {
		JobDetail job = JobBuilder.newJob(jobClass).withIdentity(jobKey, JOB_GROUP_NAME)
				.usingJobData(JobDataMapSupport.newJobDataMap(maps)).build();
		return job;
	}

	public static void deleteJob(String jobKey) throws SchedulerException {
		TriggerKey triggerKey = new TriggerKey(jobKey);
		schedule.pauseTrigger(triggerKey);
		schedule.unscheduleJob(triggerKey);
		boolean rs = schedule.deleteJob(new JobKey(jobKey));
		if(!rs){
			logger.warn("delete job failed the job info was not found ,maybe it has deleted.");
		}
	}

	public static Trigger createSimpleTrigger(int intervalInMinutes,String keyName,
			Date startTime) {
		Trigger trigger = null;
		if (startTime == null) {
			trigger = TriggerBuilder
					.newTrigger()
					.withSchedule(
							SimpleScheduleBuilder
									.simpleSchedule()
									.withIntervalInMinutes(intervalInMinutes)
									.withMisfireHandlingInstructionIgnoreMisfires().repeatForever())
					.build();
		} else {
			trigger = TriggerBuilder
					.newTrigger().withIdentity(keyName)
					.startAt(startTime)
					.withSchedule(
							SimpleScheduleBuilder
									.simpleSchedule()
									.withIntervalInMinutes(intervalInMinutes)
									.withMisfireHandlingInstructionIgnoreMisfires().repeatForever())
					.build();
		}
		return trigger;
	}

	public static Trigger createCronTrigger(String cron, String keyName,Date startTime) {
		Trigger trigger = TriggerBuilder
				.newTrigger()
				.startAt(startTime).withIdentity(keyName)
				.withSchedule(
						CronScheduleBuilder.cronSchedule(cron)
								.withMisfireHandlingInstructionIgnoreMisfires())
				.build();
		return trigger;
	}

}
