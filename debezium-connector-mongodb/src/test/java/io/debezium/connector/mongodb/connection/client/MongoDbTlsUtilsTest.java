/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.connection.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPairGenerator;
import java.util.Base64;
import java.util.Optional;

import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import io.debezium.config.Configuration;
import io.debezium.connector.mongodb.MongoDbConnectorConfig;

public class MongoDbTlsUtilsTest {

    private static final String TEST_CERTIFICATE = """
            -----BEGIN CERTIFICATE-----
            MIIDKzCCAhOgAwIBAgIUasoPCQmxVsB1pHcu26t1oFXqPwMwDQYJKoZIhvcNAQEL
            BQAwJTEjMCEGA1UEAwwaUmlzaW5nV2F2ZSBNb25nb0RCIFRlc3QgQ0EwHhcNMjYw
            ODE4MTAxMTIzWhcNMjYwODE5MTAxMTIzWjAlMSMwIQYDVQQDDBpSaXNpbmdXYXZl
            IE1vbmdvREIgVGVzdCBDQTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEB
            AK4WHBdiDKGN+g9VNPT+Q6saCT2273vsBU9y55JybUQac5Yk6rRJMFgBL5L6hr18
            QuYUgTtr7PhCvuzHhnMkIiLxYMKOdwoIPqmi7tvPDhJPcQZV4DOL2HreqQkMAA6o
            6TMUSnF8CBsvFr0mmiJ6IVyt/4wD+ec+7XczaPA+FDzmnj5wLdZ1/OkYxNiVri1g
            NnG41U8iKvpDp1QzV8fHgko3+qXgXvuFG+kLa4HoPGauuOdfUXMpnEHEfBYfljzM
            ayMOTtiYlbMLjvo9CFhegpTB/rtENVUYr8hx+CsJ1Z/iOOJrtWL0OPnz4nzCbpC7
            OMXudPuJM5qYWGaMV8fxz1cCAwEAAaNTMFEwHQYDVR0OBBYEFNg2m6oK6npxoFd7
            5sHMQpdApv1NMB8GA1UdIwQYMBaAFNg2m6oK6npxoFd75sHMQpdApv1NMA8GA1Ud
            EwEB/wQFMAMBAf8wDQYJKoZIhvcNAQELBQADggEBABwehLFu4Che+Rh0gLuSQg/W
            0o0wQuf6li8jgsAHn9sBUVnl/hS86GhvmXQoBD3UWELclFqCAwPtASXj+3182BEa
            Pd4AG+GHOPF0DDkPJgBalcLgoNHAaDqrTeThaYLAU9vwQGYHPVv19LzEKsPdLIJt
            hJiSERZb9FtZ4pLU8jUkve8WXhyXYgQWnm2uC9f23+OcNLk+ebHPUs0I+/AMCPL5
            h2ep7a9OdjDVWYY1a9dwW279FUBZQUgeZy6fk8LQEOjsY9bmX63ZEV2GESbfoZaQ
            JoXqq9NyqHZofzVRtqCKMu8Kvmcj3CTLib1Sd9azOhNPTOahXfpbwiiftwZIO2I=
            -----END CERTIFICATE-----
            """;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void extractsTlsFilesAndPreservesOtherUriOptions() {
        String connectionString = "mongodb://localhost/?tls=true&tlsCAFile=%2Fetc%2Fmongo+certs%2Fca.pem"
                + "&tlsCertificateKeyFile=%2Fetc%2Fmongo+certs%2Fclient.pem&replicaSet=rs0";

        var tlsFiles = MongoDbTlsUtils.tlsFiles(connectionString);

        assertEquals(Path.of("/etc/mongo certs/ca.pem"), tlsFiles.caFile().orElseThrow());
        assertEquals(Path.of("/etc/mongo certs/client.pem"), tlsFiles.certificateKeyFile().orElseThrow());
        assertEquals(
                "mongodb://localhost/?tls=true&replicaSet=rs0",
                MongoDbTlsUtils.withoutTlsFileOptions(connectionString));
    }

    @Test
    public void parsesMongoDbQueryDelimitersBeforeDecoding() {
        String connectionString = "mongodb://localhost/?tlsCAFile=%2Fetc%2Fmongo%2Fca%3Bprod.pem;replicaSet=rs0";

        var tlsFiles = MongoDbTlsUtils.tlsFiles(connectionString);

        assertEquals(Path.of("/etc/mongo/ca;prod.pem"), tlsFiles.caFile().orElseThrow());
        assertEquals(
                "mongodb://localhost/?replicaSet=rs0",
                MongoDbTlsUtils.withoutTlsFileOptions(connectionString));
    }

    @Test
    public void clientFactoryConsumesPemOptionsDirectlyFromConnectionString() throws Exception {
        Path caFile = temporaryFolder.newFile("factory-ca.pem").toPath();
        Files.writeString(caFile, TEST_CERTIFICATE);
        var config = Configuration.create()
                .with(
                        MongoDbConnectorConfig.CONNECTION_STRING,
                        "mongodb://localhost/?replicaSet=rs0&tlsCAFile=" + caFile)
                .build();

        var settings = new DefaultMongoDbClientFactory(config).getMongoClientSettings();

        assertTrue(settings.getSslSettings().isEnabled());
        assertNotNull(settings.getSslSettings().getContext());
    }

    @Test
    public void createsSslContextFromCertificatePem() throws Exception {
        Path caFile = temporaryFolder.newFile("ca.pem").toPath();
        Files.writeString(caFile, TEST_CERTIFICATE);

        var sslContext = MongoDbTlsUtils.createTlsSslContext(
                "mongodb://localhost/?tls=true",
                new MongoDbTlsUtils.TlsFiles(Optional.of(caFile), Optional.empty()));

        assertNotNull(sslContext.getSocketFactory());
    }

    @Test
    public void rejectsMalformedPem() throws Exception {
        Path caFile = temporaryFolder.newFile("malformed-ca.pem").toPath();
        Files.writeString(
                caFile,
                "-----BEGIN CERTIFICATE-----\nnot-base64!\n-----END CERTIFICATE-----\n");

        var error = assertThrows(
                IllegalArgumentException.class,
                () -> MongoDbTlsUtils.createTlsSslContext(
                        "mongodb://localhost/?tls=true",
                        new MongoDbTlsUtils.TlsFiles(Optional.of(caFile), Optional.empty())));

        assertTrue(error.getCause().getMessage().contains("Invalid PEM content"));
    }

    @Test
    public void createsSslContextFromPkcs8PrivateKey() throws Exception {
        var keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        Path clientPem = writeClientPem("client-pkcs8.pem", "PRIVATE KEY", keyPair.getPrivate().getEncoded());

        assertClientPemCreatesSslContext(clientPem);
    }

    @Test
    public void createsSslContextFromPkcs1RsaPrivateKey() throws Exception {
        var keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        var privateKeyInfo = PrivateKeyInfo.getInstance(keyPair.getPrivate().getEncoded());
        byte[] pkcs1 = privateKeyInfo.parsePrivateKey().toASN1Primitive().getEncoded();
        Path clientPem = writeClientPem("client-pkcs1.pem", "RSA PRIVATE KEY", pkcs1);

        assertClientPemCreatesSslContext(clientPem);
    }

    private void assertClientPemCreatesSslContext(Path clientPem) {
        var sslContext = MongoDbTlsUtils.createTlsSslContext(
                "mongodb://localhost/?tls=true",
                new MongoDbTlsUtils.TlsFiles(Optional.empty(), Optional.of(clientPem)));

        assertNotNull(sslContext.getSocketFactory());
    }

    private Path writeClientPem(String fileName, String keyLabel, byte[] key) throws Exception {
        String privateKey = Base64.getMimeEncoder(64, new byte[]{ '\n' }).encodeToString(key);
        Path path = temporaryFolder.newFile(fileName).toPath();
        Files.writeString(
                path,
                TEST_CERTIFICATE
                        + "\n-----BEGIN " + keyLabel + "-----\n"
                        + privateKey
                        + "\n-----END " + keyLabel + "-----\n");
        return path;
    }
}
