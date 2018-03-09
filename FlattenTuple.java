package com.linkedin.beam.transform;


import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.RawUnionValue;
import org.apache.beam.sdk.transforms.join.UnionCoder;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;

import java.util.ArrayList;
import java.util.List;

/**
 * In case we need this in the future.
 */
public class FlattenTuple extends PTransform<PCollectionTuple, PCollection<CoGbkResult>> {
  public static FlattenTuple create() {
    return new FlattenTuple();
  }

  private FlattenTuple() { }

  @Override
  public PCollection<CoGbkResult> expand(PCollectionTuple input) {
    // First build the union coder.
    // TODO: Look at better integration of union types with the
    // schema specified in the input.
    List<Coder<?>> codersList = new ArrayList<>();
    for (PCollection<?> pCollection: input.getAll().values()) {
      codersList.add(pCollection.getCoder());
    }
    UnionCoder unionCoder = UnionCoder.of(codersList);
    PCollectionList<RawUnionValue> unionTables =
        PCollectionList.empty(input.getPipeline());

    return null;
  }

  private <V> PCollection<RawUnionValue> makeUnionTable(
      final int index,
      PCollection<V> pCollection,
      Coder<RawUnionValue> unionTableEncoder) {

    return pCollection
        .apply("MakeUnionTable" + index, ParDo.of(new ConstructUnionTableFn<>(index)))
        .setCoder(unionTableEncoder);
  }

  /**
   * A DoFn to construct a UnionTable.
   */
  private static class ConstructUnionTableFn<V> extends
      DoFn<V, RawUnionValue> {
    private final int index;

    public ConstructUnionTableFn(int index) {
      this.index = index;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      V v = c.element();
      c.output(new RawUnionValue(index, v));
    }
  }


}
