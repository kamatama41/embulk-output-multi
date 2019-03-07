package org.embulk.output.multi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.embulk.config.TaskReport;

import java.util.List;

class TaskReports {
    private final List<TaskReport> reports;

    @JsonCreator
    TaskReports(@JsonProperty("reports") List<TaskReport> reports) {
        this.reports = reports;
    }

    @JsonProperty("reports")
    List<TaskReport> getReports() {
        return reports;
    }

    TaskReport get(int index) {
        return reports.get(index);
    }
}
