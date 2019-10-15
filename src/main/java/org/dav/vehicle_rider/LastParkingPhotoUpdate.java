package org.dav.vehicle_rider;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.LocalDate;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.dav.config.Config;
import org.dav.vehicle_rider.cassandra.RideSummary;
import org.slf4j.Logger;

import java.util.UUID;

public class LastParkingPhotoUpdate extends PTransform<PCollection<RideSummary>, PCollection<RideSummary>> {
    private static final long serialVersionUID = 1L;
    private LastParkingPhotoUpdateDoFn lastParkingPhotoUpdateDoFn;

    public LastParkingPhotoUpdate(Config config, Logger log) {
        this.lastParkingPhotoUpdateDoFn =
                new LastParkingPhotoUpdateDoFn(config, log);
    }

    @Override
    public PCollection<RideSummary> expand(PCollection<RideSummary> input) {
        return input.apply("expand Ride summaries", ParDo.of(this.lastParkingPhotoUpdateDoFn));
    }
}

class LastParkingPhotoUpdateDoFn extends CassandraDoFnBase<RideSummary, RideSummary> {
    private static final long serialVersionUID = 1L;
    private static final String QUERY =
            "UPDATE vehicle_rider.vehicle_stats_daily SET last_parking_image_url=? WHERE owner_id=? AND vehicle_id=? AND date=?";

    public LastParkingPhotoUpdateDoFn(Config config, Logger log) {
        super(config.cassandraSeed(), QUERY, log);
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        try {
            RideSummary rideSummary = context.element();
            String parkingPhoto = rideSummary.parkingImageUrl;
            UUID ownerId = rideSummary.ownerId;
            UUID vehicleId = rideSummary.vehicleId;
            LocalDate date = rideSummary.getEffectiveDate();
            BoundStatement bound = _prepared.bind(parkingPhoto, ownerId, vehicleId, date);
            this.executeBoundStatement(bound);
            context.output(rideSummary);
        } catch (Exception ex) {
            _log.error("error while trying to update Last parking photo",  ex);
        }
    }
}
