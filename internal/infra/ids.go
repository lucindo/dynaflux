// Copyright 2026 The Dynaflux Authors
// SPDX-License-Identifier: Apache-2.0

package infra

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
)

// hashDigits is how many hex digits of a name's SHA-256 [namePart] appends.
const hashDigits = 8

// namePart returns the ID part of a SQL name ([a-z][a-z0-9_]*): its PascalCase form,
// page_views to PageViews. PascalCase maps two names to one part only when a "_" isn't
// followed by a letter (a_1 and a1), so such a name also gets "X" and 8 hex digits of its
// SHA-256, which depend on the name alone: adding a stream never renames another resource.
func namePart(name string) string {
	var b strings.Builder
	injective := true
	for i, w := range strings.Split(name, "_") {
		if w == "" || (i > 0 && (w[0] < 'a' || w[0] > 'z')) {
			injective = false
		}
		if w != "" {
			b.WriteString(strings.ToUpper(w[:1]) + w[1:])
		}
	}
	if !injective {
		sum := sha256.Sum256([]byte(name))
		b.WriteString("X" + hex.EncodeToString(sum[:])[:hashDigits])
	}
	return b.String()
}

// IDs of the resources that have no name part.
const (
	idMeta             = "Meta"
	idBucket           = "Bucket"
	idProjector        = "Projector" // the store's projector when runtime.split is store
	idSetup            = "Setup"
	idCatalog          = "Catalog"
	idBackfill         = "Backfill"
	idBackfillMachine  = "BackfillStateMachine"
	idArchiveWorkGroup = "ArchiveWorkGroup"
	idArchiveSchema    = "ArchiveSchema"
	// idArchive owns the log group and the role of the delivery streams, and
	// idArchiveOptimizer the role of the table optimizers.
	idArchive          = "Archive"
	idArchiveOptimizer = "ArchiveOptimizer"
	idDashboard        = "Dashboard"
)

func streamID(stream string) string {
	return "Stream" + namePart(stream)
}

func viewID(view string, version int) string {
	return "View" + namePart(view) + "V" + strconv.Itoa(version)
}

// streamProjectorID is the ID of a stream's projector when runtime.split is stream.
func streamProjectorID(stream string) string {
	return idProjector + namePart(stream)
}

func lookupID(stream string) string {
	return "Lookup" + namePart(stream)
}

func mappingID(stream string) string {
	return "Mapping" + namePart(stream)
}

func logStreamID(stream string) string {
	return "LogStream" + namePart(stream)
}

func deliveryStreamID(stream string) string {
	return "Firehose" + namePart(stream)
}

// alarmID is the ID of the alarm on signal of the resource with ID subject, or of the whole
// store when subject is empty.
func alarmID(signal, subject string) string {
	return "Alarm" + signal + subject
}

func logsID(owner string) string {
	return "Logs" + owner
}

func roleID(owner string) string {
	return "Role" + owner
}
