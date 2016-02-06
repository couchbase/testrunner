package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"flag"
	"log"
	"math/big"
	"os"
	"time"
	"io/ioutil"
	"fmt"
	"net"
)

var keyLength = 2048

func mustNoErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

var earlyNotBefore = time.Date(2013, 1, 1, 0, 0, 0, 0, time.UTC)

// that's max date that current golang x509 code supports
var earlyNotAfter = time.Date(2049, 12, 31, 23, 59, 59, 0, time.UTC)

func pemIfy(path string, octets []byte, pemType string) {
	f, err := os.Create(path)
	mustNoErr(err)

	defer f.Close()

	pem.Encode(f, &pem.Block{
		Type:  pemType,
		Bytes: octets,
	})
}

func derToPKey(octets []byte) (pkey *rsa.PrivateKey) {
	pkey, err := x509.ParsePKCS1PrivateKey(octets)
	if err == nil {
		return
	}

	pkeyInt, err2 := x509.ParsePKCS8PrivateKey(octets)
	pkey, rsaPKey := pkeyInt.(*rsa.PrivateKey)
	if err2 == nil && !rsaPKey {
		err2 = errors.New("only rsa keys are supported yet")
	}
	if err2 == nil {
		return
	}

	log.Printf("Failed to parse pkey: %s\nOther error is:", err)
	log.Fatal(err2)
	panic("cannot happen")
}

func createCertificate(commonName string, final bool, parent *x509.Certificate, parentPKey *rsa.PrivateKey) ([]byte, *rsa.PrivateKey) {
	pkey, err := rsa.GenerateKey(rand.Reader, keyLength)
	mustNoErr(err)

	template := x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		IsCA:         !final,
		NotBefore:    earlyNotBefore,
		NotAfter:     earlyNotAfter,
		Subject:      pkix.Name{
			CommonName: commonName,
		},
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	if final {
		if ip := net.ParseIP(commonName); ip != nil {
			template.IPAddresses = []net.IP{ip}
		}
	}

	template.KeyUsage = x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature
	if !final {
		template.KeyUsage = template.KeyUsage | x509.KeyUsageCertSign
	}

	if parent != nil {
		template.SignatureAlgorithm = parent.SignatureAlgorithm
	}

	signer := &template
	publicKey := &pkey.PublicKey
	privateKey := pkey

	if parent != nil {
		if parentPKey == nil {
			panic("parentPKey should be supplied")
		}
		signer = parent
		privateKey = parentPKey
	}

	certDer, err := x509.CreateCertificate(rand.Reader, &template, signer, publicKey, privateKey)
	mustNoErr(err)
	return certDer, pkey
}

func readPemFile(path, pemType string) ([]byte) {
	bytes, err := ioutil.ReadFile(path)
	mustNoErr(err)

	block, rest := pem.Decode(bytes)

	if (string)(rest) != "" || block == nil || block.Type != pemType {
		log.Fatal("garbage in file")
	}
	return block.Bytes
}


func readParent(signWith string) (*x509.Certificate, *rsa.PrivateKey) {
	if signWith == "" {
		return nil, nil
	}

	certBytes := readPemFile(signWith + ".crt", "CERTIFICATE")
	pkeyBytes := readPemFile(signWith + ".key", "RSA PRIVATE KEY")

	caCert, err := x509.ParseCertificate(certBytes)
	mustNoErr(err)

	pkey := derToPKey(pkeyBytes)
	return caCert, pkey
}

func init() {
}

func main() {
	var commonName string
	var signWith string
	var storeTo string
	var final bool

	flag.StringVar(&commonName, "common-name", "*", "common name field of certificate (hostname)")
	flag.StringVar(&signWith, "sign-with", "", "authority to sign the certificate with")
	flag.StringVar(&storeTo, "store-to", "", "where to store certificate and key")
	flag.BoolVar(&final, "final", false, "whether this is a actual node certificate")
	flag.Parse()

	parentCert, parentPKey := readParent(signWith)

	cert, pkey := createCertificate(commonName, final, parentCert, parentPKey)

	pemIfy(storeTo + ".crt", cert, "CERTIFICATE")
	pemIfy(storeTo + ".key", x509.MarshalPKCS1PrivateKey(pkey), "RSA PRIVATE KEY")
	fmt.Println("Done.")
}
