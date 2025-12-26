package org.shazaei;

import com.google.protobuf.InvalidProtocolBufferException;
import org.shazaei.protobuf.TaxiLoc;

public class ByteArrayToProtoLogMapper {

    public static TaxiLoc.TaxiLocation toProtoLog(byte[] taxiLocation) throws InvalidProtocolBufferException {
        return TaxiLoc.TaxiLocation.parseFrom(taxiLocation);
    }
}

