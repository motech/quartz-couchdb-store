package org.motechproject.quartz;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class DummyJobListener implements Job {

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        // do nothing
    }
}
