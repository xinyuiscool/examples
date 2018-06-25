package com.linkedin.beam.examples;

import com.linkedin.beam.examples.config.ConfigForExamples;
import com.linkedin.beam.values.CoGroupWithTable;
import com.linkedin.beam.values.PCollectionWithTable;
import com.linkedin.beam.values.PTable;
import com.linkedin.beam.values.ROTable;
import com.linkedin.beam.values.PReadOnlyTable;
import com.linkedin.beam.values.RWTable;
import com.linkedin.beam.values.PCollectionTableJoin;
import com.linkedin.beam.values.TableContext;
import com.linkedin.beam.io.BrooklinIO;
import com.linkedin.beam.io.BrooklinIOConfig;
import com.linkedin.beam.io.LiKafkaIO;
import com.linkedin.beam.io.LiKafkaIOConfig;
import com.linkedin.events.PageViewEvent;
import com.linkedin.identity.Profile;
import com.linkedin.identity.internal.InternalSetting;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Instant;

import static com.linkedin.beam.io.BrooklinIO.ESPRESSO;
import static com.linkedin.beam.io.LiKafkaIOConfig.ClusterName.TRACKING;

public class TableExample {

  public static void main(String[] args) {
    final SamzaPipelineOptions pipelineOpts = ConfigForExamples.getSamzaPipelineOptions("table-example");
    final LiKafkaIOConfig kafkaConfig = ConfigForExamples.getLiKafkaIOConfig();
    final BrooklinIOConfig brooklinConfig = ConfigForExamples.getBrooklinIOConfig();
    final Pipeline pipeline = Pipeline.create(pipelineOpts);

    // A Brooklin stream
    final PCollection<KV<String, InternalSetting>> internalSettings = pipeline
        .apply(BrooklinIO.<GenericRecord, InternalSetting>read()
            .withConnectorName(ESPRESSO)
            .withStream("beam-test-brooklin-internal-setting")
            .withTimestampFn(kv -> new Instant(kv.getValue().getLastModified()))
            .withConfig(brooklinConfig)
            .withoutMetadata())
        .apply(Values.create())
        .apply(WithKeys
            .of(setting -> setting.getMemberId().toString()));

    //Local RocksDb Table
    final PTable<KV<String, InternalSetting>> settingsTable =
        pipeline.apply(
            RocksDbTable.readWrite()
                .withName("internalSettings")
                .withKeyCoder(StringUtf8Coder.of())
                .withValueCoder(AvroCoder.of(PageViewEvent.class))
                .withInput(internalSettings));

    // Remote Db Table
    final PReadOnlyTable<KV<String, Profile>> profileTable =
        pipeline.apply(
            EspressoTable.readOnly()
                .withDb("isb")
                .withTable("profile"));

    // main input
    PCollection<KV<String, PageViewEvent>> pageView = pipeline
        .apply(LiKafkaIO.<PageViewEvent>read()
            .withTopic("PageViewEvent")
            .withConsumerConfig(kafkaConfig.getConsumerConfig(TRACKING))
            .withoutMetadata());

    // main input process with table
    pageView
        .apply(TableParDo
            .of(
                new DoFn<KV<String, PageViewEvent>, String>() {
                  @ProcessElement
                  public void processElement(ProcessContext c,
                                             @TableContext.Inject TableContext tc) {
                    String memberId = c.element().getKey();

                    //table lookup
                    RWTable<String, InternalSetting> settings = tc.getTable(settingsTable);
                    InternalSetting is = settings.get(memberId);
                    c.output(is.getName().toString());
                  }
                })
            .withTables(settingsTable));

    // Use the convenient helper class to do the same thing
    PCollection<String> result = PCollectionTableJoin.of(pageView, settingsTable)
        .into(TypeDescriptors.strings())
        .via((pv, setting) -> setting.getName().toString());
  }
}
