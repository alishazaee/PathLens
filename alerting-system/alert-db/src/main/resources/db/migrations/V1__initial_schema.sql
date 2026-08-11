CREATE TABLE rule (
                      id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
                      title text NOT NULL,
                      geometry_wkt text NOT NULL,
                      created_at timestamp NOT NULL DEFAULT now(),
                      updated_at timestamp NOT NULL DEFAULT now(),
                      expires_at timestamp NOT NULL,
                      identity_type text NOT NULL,
                      identity_value text NOT NULL,
                      is_active boolean NOT NULL DEFAULT true,
                      rule_type text NOT NULL,
                      is_violated boolean NOT NULL DEFAULT false
);

CREATE TABLE notification (
                              id uuid DEFAULT gen_random_uuid() PRIMARY KEY,
                              seen boolean NOT NULL DEFAULT false,
                              rule_id uuid NOT NULL,
                              created_at timestamp NOT NULL DEFAULT now(),
                              log_timestamp_hour timestamp NOT NULL,
                              CONSTRAINT fk_notification_rule
                                  FOREIGN KEY (rule_id)
                                      REFERENCES rule(id)
);

CREATE TABLE tracked_log (
                     id uuid NOT NULL DEFAULT gen_random_uuid(),
                     latitude DOUBLE PRECISION,
                     longitude DOUBLE PRECISION,
                     is_violated boolean NOT NULL,
                     rule_id uuid NOT NULL,
                     created_at timestamp NOT NULL DEFAULT now(),
                     timestamp timestamp NOT NULL,
                     CONSTRAINT pk_tracked_log PRIMARY KEY (id, rule_id),
                     CONSTRAINT fk_tracked_log_rule
                         FOREIGN KEY (rule_id)
                             REFERENCES rule(id)
) PARTITION BY HASH (rule_id);

CREATE UNIQUE INDEX uq_notification_rule_hour
    ON notification(rule_id, log_timestamp_hour);

CREATE TABLE tracked_log_p0 PARTITION OF tracked_log
    FOR VALUES WITH (MODULUS 8, REMAINDER 0);

CREATE TABLE tracked_log_p1 PARTITION OF tracked_log
    FOR VALUES WITH (MODULUS 8, REMAINDER 1);

CREATE TABLE tracked_log_p2 PARTITION OF tracked_log
    FOR VALUES WITH (MODULUS 8, REMAINDER 2);

CREATE TABLE tracked_log_p3 PARTITION OF tracked_log
    FOR VALUES WITH (MODULUS 8, REMAINDER 3);

CREATE TABLE tracked_log_p4 PARTITION OF tracked_log
    FOR VALUES WITH (MODULUS 8, REMAINDER 4);

CREATE TABLE tracked_log_p5 PARTITION OF tracked_log
    FOR VALUES WITH (MODULUS 8, REMAINDER 5);

CREATE TABLE tracked_log_p6 PARTITION OF tracked_log
    FOR VALUES WITH (MODULUS 8, REMAINDER 6);

CREATE TABLE tracked_log_p7 PARTITION OF tracked_log
    FOR VALUES WITH (MODULUS 8, REMAINDER 7);

CREATE INDEX idx_tracked_log_rule_id_created_at
    ON tracked_log(rule_id, created_at);

CREATE INDEX idx_tracked_log_created_at
    ON tracked_log(created_at);