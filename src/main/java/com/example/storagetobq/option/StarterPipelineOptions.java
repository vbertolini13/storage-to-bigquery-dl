package com.example.storagetobq.option;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface StarterPipelineOptions extends PipelineOptions {

    @Description("Input file champs on storage gcp")
    @Default.String("gs://dataflow-inputs/champs.csv")
    String getInputFileChamps();
    void setInputFileChamps(String value);
    
    @Description("Input file team bans on storage gcp")
    @Default.String("gs://dataflow-inputs/teambans.csv")
    String getInputFileBans();
    void setInputFileBans(String value);

    @Description("Output bq in gcp dataset.table")
    @Default.String("leagueoflegends.champion_bans")
    String getTableStagingFileLines();
    void setTableStagingFileLines(String value);
    
    @Description("Id project.")
    @Default.String("civil-medley-236513")
    String getProjectId();
    void setProjectId(String value);

}
