/* SPDX-License-Identifier: GPL-2.0-only */
/* Child-local fixture: model an empty store without reading or changing it. */
int gnutls_certificate_set_x509_system_trust(void *credentials) {
    (void)credentials;
    return 0;
}
