package com.example.storagetobq.transform;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

import com.example.storagetobq.dofn.CoGbkToChampBanDoFn;
import com.example.storagetobq.domain.Ban;
import com.example.storagetobq.domain.Champ;
import com.example.storagetobq.domain.ChampBan;

public class CoGbkToChampBanTransform extends PTransform<PCollection<KV<String, CoGbkResult>>,PCollection<KV<String, ChampBan>>>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 6504996652417778031L;
	private TupleTag<Champ> champTupleTag;
	private TupleTag<Ban> banTupleTag;
	
	public CoGbkToChampBanTransform(TupleTag<Champ> champTupleTag, TupleTag<Ban> banTupleTag) {
		this.champTupleTag = champTupleTag;
		this.banTupleTag = banTupleTag;
	}
	
	@Override
	public PCollection<KV<String, ChampBan>> expand(PCollection<KV<String, CoGbkResult>> input) {
		return input.apply(ParDo.of(new CoGbkToChampBanDoFn(champTupleTag, banTupleTag)));
	}
}
