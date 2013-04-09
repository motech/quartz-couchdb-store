package org.motechproject.quartz;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class JobListener implements Job {

    private static List<Date> fireTimes = new ArrayList<Date>();

    public JobListener() {
    }

    public static List<Date> getFireTimes() {
        return fireTimes;
    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        fireTimes.add(new Date());
        if (context.getNextFireTime() == null) {
            synchronized (fireTimes) {
                fireTimes.notifyAll();
            }
        }
    }
}
