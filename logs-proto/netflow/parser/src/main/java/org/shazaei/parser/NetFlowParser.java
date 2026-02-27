package org.shazaei.parser;

import org.shazaei.log.NetFlowProto;

import java.util.regex.Pattern;

public class NetFlowParser {
    private final NetFlowProto.Log.Builder log;
    private final String rawLog;

    public NetFlowParser(NetFlowProto.Log.Builder log, String rawLog) {
        this.log = log;
        this.rawLog = rawLog;
    }

    public void parse() {
        String[] fields = rawLog.split("\\|");

        if (fields.length < 20) {
            log.addErrorSummary(NetFlowProto.Error.RECORD_SIZE_INCORRECT);
            setErrorType();
            return;
        }

        if (isNumeric(fields[12]) && Long.parseLong(fields[12]) != 0) {
            log.setTimestamp(Long.parseLong(fields[12]));
        } else {
            log.addErrorSummary(NetFlowProto.Error.INVALID_TIMESTAMP);
        }

        if (isNumeric(fields[6]) && Integer.parseInt(fields[6]) != 0) {
            log.setInBytes(Integer.parseInt(fields[6]));
        } else {
            log.addErrorSummary(NetFlowProto.Error.INVALID_IN_BYTES);
        }

        if (isNumeric(fields[8]) && Integer.parseInt(fields[8]) != 0) {
            log.setOutBytes(Integer.parseInt(fields[8]));
        } else {
            log.addErrorSummary(NetFlowProto.Error.INVALID_OUT_BYTES);
        }

        fillSrcFields(fields);
        fillDestFields(fields);

        setErrorType();
    }

    private void fillSrcFields(String[] fields) {
        String srcIpv4 = fields[0];
        if (isIpv4Valid(srcIpv4)) {
            log.setIpv4SrcAddr(srcIpv4);
        } else {
            log.addErrorSummary(NetFlowProto.Error.INVALID_SRC_IPV4);
        }

        String srcPort = fields[1];
        if (isPortNumberValid(srcPort)) {
            log.setL4SrcPort(Integer.parseInt(srcPort));
        } else {
            log.addErrorSummary(NetFlowProto.Error.INVALID_SRC_PORT);
        }
    }

    private void fillDestFields(String[] fields) {
        String destIpv4 = fields[2];
        if (isIpv4Valid(destIpv4)) {
            log.setIpv4DstAddr(destIpv4);
        } else {
            log.addErrorSummary(NetFlowProto.Error.INVALID_DEST_IPV4);
        }

        String destPort = fields[3];
        if (isPortNumberValid(destPort)) {
            log.setL4DstPort(Integer.parseInt(destPort));
        } else {
            log.addErrorSummary(NetFlowProto.Error.INVALID_DEST_PORT);
        }
    }

    private boolean isIpv4Valid(String ipv4) {
        if (ipv4 == null || ipv4.trim().isEmpty()) return false;
        Pattern pattern = Pattern.compile("^(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])$");
        return pattern.matcher(ipv4.trim()).matches();
    }

    private boolean isPortNumberValid(String portStr) {
        if (!isNumeric(portStr)) return false;
        try {
            int portNumber = Integer.parseInt(portStr);
            return portNumber >= 0 && portNumber <= 65535;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private boolean isNumeric(String str) {
        if (str == null || str.trim().isEmpty()) {
            return false;
        }
        return str.trim().matches("\\d+");
    }

    private void setErrorType(){
        log.setRawRecord(rawLog);
        if(log.getErrorSummaryList().contains(NetFlowProto.Error.INVALID_TIMESTAMP)
            || log.getErrorSummaryList().contains(NetFlowProto.Error.RECORD_SIZE_INCORRECT)){
            log.setErrorType(NetFlowProto.ErrorType.HARD);
        } else {
            log.setErrorType(NetFlowProto.ErrorType.SOFT);
        }
    }
}
