/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.example.storagetobq;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

import com.example.storagetobq.domain.Ban;
import com.example.storagetobq.domain.Champ;
import com.example.storagetobq.option.StarterPipelineOptions;
import com.example.storagetobq.persistence.ChampBanSchema;
import com.example.storagetobq.transform.BanToKeyValueTransform;
import com.example.storagetobq.transform.ChampToKeyValueTransform;
import com.example.storagetobq.transform.CoGbkToChampBanTransform;
import com.example.storagetobq.transform.KVStringLongToChampBanTransform;
import com.example.storagetobq.transform.StringToBanTransform;
import com.example.storagetobq.transform.StringToChampTransform;
import com.example.storagetobq.util.Constants;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StarterPipeline {

    public static void main(String[] args) {
        log.info(Constants.START_PROCESS);

        TupleTag<Champ> champTupleTag = new TupleTag<>();
        TupleTag<Ban> banTupleTag = new TupleTag<>();

        StarterPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(StarterPipelineOptions.class);
        Pipeline p = Pipeline.create(options);

        PCollection<KV<String, Champ>> kvChamps = p.apply(Constants.READ_CSV_CHAMP, TextIO.read().from(options.getInputFileChamps()))
        	.apply(Constants.READ_CSV_CHAMP, new StringToChampTransform())
        	.apply(Constants.CHAMP_TO_KV, new ChampToKeyValueTransform());
        
        PCollection<KV<String, Ban>> kvBans = p.apply(Constants.READ_CSV_BAN, TextIO.read().from(options.getInputFileBans()))
        	.apply(Constants.LINE_TO_BAN, new StringToBanTransform())
        	.apply(Constants.BAN_TO_KV, new BanToKeyValueTransform());
        
        KeyedPCollectionTuple.of(champTupleTag, kvChamps).and(banTupleTag, kvBans)
        	.apply(Constants.CGK_CHAMP_BAN, CoGroupByKey.<String>create())
        	.apply(Constants.CGK_CHAMPBAN, new CoGbkToChampBanTransform(champTupleTag, banTupleTag))
            .apply(Constants.COUNT_BY_CGK_CHAMPBAN, Count.perKey())
            .apply(Constants.KV_SCHEMA, new KVStringLongToChampBanTransform())
    		.apply(Constants.WRITE_CHAMPBAN, BigQueryIO.writeTableRows()
    				.to(options.getTableStagingFileLines())
    				.withSchema(ChampBanSchema.getTableSchema())
    				.withCreateDisposition(CREATE_IF_NEEDED)
    				.withWriteDisposition(WRITE_TRUNCATE));
        
        
        p.run().waitUntilFinish();

        log.info(Constants.FINISHED_PROCESS);
    }
}
