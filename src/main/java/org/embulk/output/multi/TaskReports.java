package org.embulk.output.multi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.embulk.config.TaskReport;

import java.util.Map;

class TaskReports {
    private final Map<String, TaskReport> reports;

    @JsonCreator
    TaskReports(@JsonProperty("reports") Map<String, TaskReport> reports) {
        this.reports = reports;
    }

    @JsonProperty("reports")
    Map<String, TaskReport> getReports() {
        return reports;
    }

    TaskReport get(String tag) {
        return reports.get(tag);
    }
}
