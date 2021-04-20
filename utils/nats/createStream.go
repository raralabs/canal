package nats

/*
// Create a stream attached to the NATS subject "foo.*" that is replicated to
// all the brokers in the cluster. ErrStreamExists is returned if a stream with
// the given name already exists.
client.CreateStream(context.Background(), "foo.*", "my-stream", lift.MaxReplication())

We can also configure different properties of the stream with options, which
allow overriding the server settings.

// Create a stream and set the rentention to 134217728 bytes (128MB) and enable
// stream compaction.
client.CreateStream(context.Background(), subject, name,
lift.RetentionMaxBytes(134217728), lift.CompactEnabled(true))
*/
