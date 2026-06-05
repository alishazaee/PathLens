
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

                              CONSTRAINT fk_notification_rule
                                  FOREIGN KEY (rule_id)
                                      REFERENCES rule(id)
);

CREATE TABLE log (
                     id uuid NOT NULL DEFAULT gen_random_uuid(),
                     latitude DOUBLE PRECISION,
                     longitude DOUBLE PRECISION,
                     is_violated boolean NOT NULL,
                     created_at timestamp NOT NULL DEFAULT now(),
                     rule_id uuid NOT NULL,
                     CONSTRAINT pk_log PRIMARY KEY (id, rule_id),
                     CONSTRAINT fk_log_rule
                         FOREIGN KEY (rule_id)
                             REFERENCES rule(id)
) PARTITION BY HASH (rule_id);

CREATE TABLE log_p0 PARTITION OF log
    FOR VALUES WITH (MODULUS 8, REMAINDER 0);

CREATE TABLE log_p1 PARTITION OF log
    FOR VALUES WITH (MODULUS 8, REMAINDER 1);

CREATE TABLE log_p2 PARTITION OF log
    FOR VALUES WITH (MODULUS 8, REMAINDER 2);

CREATE TABLE log_p3 PARTITION OF log
    FOR VALUES WITH (MODULUS 8, REMAINDER 3);

CREATE TABLE log_p4 PARTITION OF log
    FOR VALUES WITH (MODULUS 8, REMAINDER 4);

CREATE TABLE log_p5 PARTITION OF log
    FOR VALUES WITH (MODULUS 8, REMAINDER 5);

CREATE TABLE log_p6 PARTITION OF log
    FOR VALUES WITH (MODULUS 8, REMAINDER 6);

CREATE TABLE log_p7 PARTITION OF log
    FOR VALUES WITH (MODULUS 8, REMAINDER 7);

CREATE INDEX idx_log_rule_id_created_at
    ON log(rule_id, created_at);

CREATE INDEX idx_log_created_at
    ON log(created_at);