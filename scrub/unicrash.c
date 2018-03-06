/*
 * Copyright (C) 2018 Oracle.  All Rights Reserved.
 *
 * Author: Darrick J. Wong <darrick.wong@oracle.com>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it would be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write the Free Software Foundation,
 * Inc.,  51 Franklin St, Fifth Floor, Boston, MA  02110-1301, USA.
 */
#include "xfs.h"
#include <stdint.h>
#include <stdlib.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/statvfs.h>
#include <strings.h>
#include <unicode/ustring.h>
#include <unicode/unorm2.h>
#include "path.h"
#include "xfs_scrub.h"
#include "common.h"

/*
 * Detect collisions of Unicode-normalized names.
 *
 * Record all the name->ino mappings in a directory/xattr, with a twist!
 * The twist is that we perform unicode normalization on every name we
 * see, so that we can warn about a directory containing more than one
 * directory entries that normalize to the same Unicode string.  These
 * entries are at best a sign of Unicode mishandling, or some sort of
 * weird name substitution attack if the entries do not point to the
 * same inode.  Warn if we see multiple dirents that do not all point to
 * the same inode.
 *
 * For extended attributes we perform the same collision checks on the
 * attribute, though any collision is enough to trigger a warning.
 *
 * We flag these collisions as warnings and not errors because XFS
 * treats names as a sequence of arbitrary nonzero bytes.  While a
 * Unicode collision is not technically a filesystem corruption, we
 * ought to say something if there's a possibility for misleading a
 * user.
 *
 * To normalize, we use Unicode NFKC.  We use the composing
 * normalization mode (e.g. "E WITH ACUTE" instead of "E" then "ACUTE")
 * because that's what W3C (and in general Linux) uses.  This enables us
 * to detect multiple object names that normalize to the same name and
 * could be confusing to users.  Furthermore, we use the compatibility
 * mode to detect names with compatible but different code points to
 * strengthen those checks.
 */

struct name_entry {
	struct name_entry	*next;

	/* NFKC normalized name */
	UChar			*normstr;
	size_t			normstrlen;

	xfs_ino_t		ino;

	/* Raw UTF8 name */
	size_t			namelen;
	char			name[0];
};
#define NAME_ENTRY_SZ(nl)	(sizeof(struct name_entry) + 1 + \
				 (nl * sizeof(uint8_t)))

struct unicrash {
	struct scrub_ctx	*ctx;
	const UNormalizer2	*normalizer;
	bool			compare_ino;
	size_t			nr_buckets;
	struct name_entry	*buckets[0];
};
#define UNICRASH_SZ(nr)		(sizeof(struct unicrash) + \
				 (nr * sizeof(struct name_entry *)))

/* Things to complain about in Unicode naming. */

/*
 * Multiple names resolve to the same normalized string and therefore render
 * identically.
 */
#define UNICRASH_NOT_UNIQUE	(1 << 0)

/* Name contains directional overrides. */
#define UNICRASH_BIDI_OVERRIDE	(1 << 1)

/* Name mixes left-to-right and right-to-left characters. */
#define UNICRASH_BIDI_MIXED	(1 << 2)

/* Control characters in name. */
#define UNICRASH_CONTROL_CHAR	(1 << 3)

/* Invisible characters.  Only a problem if we have collisions. */
#define UNICRASH_ZERO_WIDTH	(1 << 4)

/*
 * We only care about validating utf8 collisions if the underlying
 * system configuration says we're using utf8.  If the language
 * specifier string used to output messages has ".UTF-8" somewhere in
 * its name, then we conclude utf8 is in use.  Otherwise, no checking is
 * performed.
 *
 * Most modern Linux systems default to utf8, so the only time this
 * check will return false is if the administrator configured things
 * this way or if things are so messed up there is no locale data at
 * all.
 */
#define UTF8_STR		".UTF-8"
#define UTF8_STRLEN		(sizeof(UTF8_STR) - 1)
static bool
is_utf8_locale(void)
{
	const char		*msg_locale;
	static int		answer = -1;

	if (answer != -1)
		return answer;

	msg_locale = setlocale(LC_MESSAGES, NULL);
	if (msg_locale == NULL)
		return false;

	if (strstr(msg_locale, UTF8_STR) != NULL)
		answer = 1;
	else
		answer = 0;
	return answer;
}

/*
 * Generate normalized form of the name.
 * If this fails, just forget everything; this is an advisory checker.
 */
static bool
name_entry_compute_checknames(
	struct unicrash		*uc,
	struct name_entry	*entry)
{
	UChar			*normstr;
	UChar			*unistr;
	int32_t			normstrlen;
	int32_t			unistrlen;
	UErrorCode		uerr = U_ZERO_ERROR;

	/* Convert bytestr to unistr for normalization */
	u_strFromUTF8(NULL, 0, &unistrlen, entry->name, entry->namelen, &uerr);
	if (uerr != U_BUFFER_OVERFLOW_ERROR)
		return false;
	uerr = U_ZERO_ERROR;
	unistr = calloc(unistrlen + 1, sizeof(UChar));
	if (!unistr)
		return false;
	u_strFromUTF8(unistr, unistrlen, NULL, entry->name, entry->namelen,
			&uerr);
	if (U_FAILURE(uerr))
		goto out_unistr;

	/* Normalize the string. */
	normstrlen = unorm2_normalize(uc->normalizer, unistr, unistrlen, NULL,
			0, &uerr);
	if (uerr != U_BUFFER_OVERFLOW_ERROR)
		goto out_unistr;
	uerr = U_ZERO_ERROR;
	normstr = calloc(normstrlen + 1, sizeof(UChar));
	if (!normstr)
		goto out_unistr;
	unorm2_normalize(uc->normalizer, unistr, unistrlen, normstr, normstrlen,
			&uerr);
	if (U_FAILURE(uerr))
		goto out_normstr;

	entry->normstr = normstr;
	entry->normstrlen = normstrlen;
	free(unistr);
	return true;

out_normstr:
	free(normstr);
out_unistr:
	free(unistr);
	return false;
}

/* Create a new name entry, returns false if we could not succeed. */
static bool
name_entry_create(
	struct unicrash		*uc,
	const char		*name,
	xfs_ino_t		ino,
	struct name_entry	**entry)
{
	struct name_entry	*new_entry;
	size_t			namelen = strlen(name);

	/* Create new entry */
	new_entry = calloc(NAME_ENTRY_SZ(namelen), 1);
	if (!new_entry)
		return false;
	new_entry->next = NULL;
	new_entry->ino = ino;
	memcpy(new_entry->name, name, namelen);
	new_entry->name[namelen] = 0;
	new_entry->namelen = namelen;

	/* Normalize name to find collisions. */
	if (!name_entry_compute_checknames(uc, new_entry))
		goto out;

	*entry = new_entry;
	return true;

out:
	free(new_entry);
	return false;
}

/* Free a name entry */
static void
name_entry_free(
	struct name_entry	*entry)
{
	free(entry->normstr);
	free(entry);
}

/* Adapt the dirhash function from libxfs, avoid linking with libxfs. */

#define rol32(x, y)		(((x) << (y)) | ((x) >> (32 - (y))))

/*
 * Implement a simple hash on a character string.
 * Rotate the hash value by 7 bits, then XOR each character in.
 * This is implemented with some source-level loop unrolling.
 */
static xfs_dahash_t
name_entry_hash(
	struct name_entry	*entry)
{
	uint8_t			*name;
	size_t			namelen;
	xfs_dahash_t		hash;

	name = (uint8_t *)entry->normstr;
	namelen = entry->normstrlen * sizeof(UChar);

	/*
	 * Do four characters at a time as long as we can.
	 */
	for (hash = 0; namelen >= 4; namelen -= 4, name += 4)
		hash = (name[0] << 21) ^ (name[1] << 14) ^ (name[2] << 7) ^
		       (name[3] << 0) ^ rol32(hash, 7 * 4);

	/*
	 * Now do the rest of the characters.
	 */
	switch (namelen) {
	case 3:
		return (name[0] << 14) ^ (name[1] << 7) ^ (name[2] << 0) ^
		       rol32(hash, 7 * 3);
	case 2:
		return (name[0] << 7) ^ (name[1] << 0) ^ rol32(hash, 7 * 2);
	case 1:
		return (name[0] << 0) ^ rol32(hash, 7 * 1);
	default: /* case 0: */
		return hash;
	}
}

/*
 * Check a name for suspicious elements that have appeared in filename
 * spoofing attacks.  This includes names that mixed directions or contain
 * direction overrides control characters, both of which have appeared in
 * filename spoofing attacks.
 */
static void
name_entry_examine(
	struct name_entry	*entry,
	unsigned int		*badflags)
{
	UChar32			uchr;
	int32_t			i;
	uint8_t			mask = 0;

	for (i = 0; i < entry->normstrlen;) {
		U16_NEXT_UNSAFE(entry->normstr, i, uchr);

		/* zero width character sequences */
		switch (uchr) {
		case 0x200B:	/* zero width space */
		case 0x200C:	/* zero width non-joiner */
		case 0x200D:	/* zero width joiner */
		case 0xFEFF:	/* zero width non breaking space */
		case 0x2060:	/* word joiner */
		case 0x2061:	/* function application */
		case 0x2062:	/* invisible times (multiply) */
		case 0x2063:	/* invisible separator (comma) */
		case 0x2064:	/* invisible plus (addition) */
			*badflags |= UNICRASH_ZERO_WIDTH;
			break;
		}

		/* control characters */
		if (u_iscntrl(uchr))
			*badflags |= UNICRASH_CONTROL_CHAR;

		switch (u_charDirection(uchr)) {
		case U_LEFT_TO_RIGHT:
			mask |= 0x01;
			break;
		case U_RIGHT_TO_LEFT:
			mask |= 0x02;
			break;
		case U_RIGHT_TO_LEFT_OVERRIDE:
			*badflags |= UNICRASH_BIDI_OVERRIDE;
			break;
		case U_LEFT_TO_RIGHT_OVERRIDE:
			*badflags |= UNICRASH_BIDI_OVERRIDE;
			break;
		default:
			break;
		}
	}

	/* mixing left-to-right and right-to-left chars */
	if (mask == 0x3)
		*badflags |= UNICRASH_BIDI_MIXED;
}

/* Initialize the collision detector. */
static bool
unicrash_init(
	struct unicrash		**ucp,
	struct scrub_ctx	*ctx,
	bool			compare_ino,
	size_t			nr_buckets)
{
	struct unicrash		*p;
	UErrorCode		uerr = U_ZERO_ERROR;

	if (!is_utf8_locale()) {
		*ucp = NULL;
		return true;
	}

	if (nr_buckets > 65536)
		nr_buckets = 65536;
	else if (nr_buckets < 16)
		nr_buckets = 16;

	p = calloc(1, UNICRASH_SZ(nr_buckets));
	if (!p)
		return false;
	p->ctx = ctx;
	p->nr_buckets = nr_buckets;
	p->compare_ino = compare_ino;
	p->normalizer = unorm2_getNFKCInstance(&uerr);
	if (U_FAILURE(uerr))
		goto out_free;
	*ucp = p;

	return true;
out_free:
	free(p);
	return false;
}

/* Initialize the collision detector for a directory. */
bool
unicrash_dir_init(
	struct unicrash		**ucp,
	struct scrub_ctx	*ctx,
	struct xfs_bstat	*bstat)
{
	/*
	 * Assume 64 bytes per dentry, clamp buckets between 16 and 64k.
	 * Same general idea as dir_hash_init in xfs_repair.
	 */
	return unicrash_init(ucp, ctx, true, bstat->bs_size / 64);
}

/* Initialize the collision detector for an extended attribute. */
bool
unicrash_xattr_init(
	struct unicrash		**ucp,
	struct scrub_ctx	*ctx,
	struct xfs_bstat	*bstat)
{
	/* Assume 16 attributes per extent for lack of a better idea. */
	return unicrash_init(ucp, ctx, false, 16 * (1 + bstat->bs_aextents));
}

/* Free the crash detector. */
void
unicrash_free(
	struct unicrash		*uc)
{
	struct name_entry	*ne;
	struct name_entry	*x;
	size_t			i;

	if (!uc)
		return;

	for (i = 0; i < uc->nr_buckets; i++) {
		for (ne = uc->buckets[i]; ne != NULL; ne = x) {
			x = ne->next;
			name_entry_free(ne);
		}
	}
	free(uc);
}

/* Complain about Unicode problems. */
static void
unicrash_complain(
	struct unicrash		*uc,
	const char		*descr,
	const char		*what,
	struct name_entry	*entry,
	unsigned int		badflags,
	struct name_entry	*dup_entry)
{
	char			*bad1 = NULL;
	char			*bad2 = NULL;

	bad1 = string_escape(entry->name);
	if (dup_entry)
		bad2 = string_escape(dup_entry->name);

	/*
	 * Most filechooser UIs do not look for bidirectional overrides when
	 * they render names.  This can result in misleading name presentation
	 * that makes "hig<rtl>gnp.sh" render like "highs.png".
	 */
	if (badflags & UNICRASH_BIDI_OVERRIDE) {
		str_warn(uc->ctx, descr,
_("Unicode name \"%s\" in %s contains suspicious text direction overrides."),
				bad1, what);
		goto out;
	}

	/*
	 * Two names that normalize to the same string will render
	 * identically even though the filesystem considers them unique
	 * names.  "cafe\xcc\x81" and "caf\xc3\xa9" have different byte
	 * sequences, but they both appear as "café".
	 */
	if (badflags & UNICRASH_NOT_UNIQUE) {
		str_warn(uc->ctx, descr,
_("Unicode name \"%s\" in %s renders identically to \"%s\"."),
				bad1, what, bad2);
		goto out;
	}

	/*
	 * Unfiltered control characters can mess up your terminal and render
	 * invisibly in filechooser UIs.
	 */
	if (badflags & UNICRASH_CONTROL_CHAR) {
		str_warn(uc->ctx, descr,
_("Unicode name \"%s\" in %s contains control characters."),
				bad1, what);
		goto out;
	}

	/*
	 * It's not considered good practice (says Unicode) to mix LTR
	 * characters with RTL characters.  The mere presence of different
	 * bidirectional characters isn't enough to trip up software, so don't
	 * warn about this too loudly.
	 */
	if (badflags & UNICRASH_BIDI_MIXED) {
		str_info(uc->ctx, descr,
_("Unicode name \"%s\" in %s mixes bidirectional characters."),
				bad1, what);
		goto out;
	}

out:
	free(bad1);
	free(bad2);
}

/*
 * Try to add a name -> ino entry to the collision detector.  The name
 * must be normalized according to Unicode NFKC rules to detect names that
 * could be confused with each other.
 */
static bool
unicrash_add(
	struct unicrash		*uc,
	struct name_entry	*new_entry,
	unsigned int		*badflags,
	struct name_entry	**existing_entry)
{
	struct name_entry	*entry;
	size_t			bucket;
	xfs_dahash_t		hash;

	/* Store name in hashtable. */
	hash = name_entry_hash(new_entry);
	bucket = hash % uc->nr_buckets;
	entry = uc->buckets[bucket];
	new_entry->next = entry;
	uc->buckets[bucket] = new_entry;

	while (entry != NULL) {
		/* Same normalization? */
		if (new_entry->normstrlen == entry->normstrlen &&
		    !u_strcmp(new_entry->normstr, entry->normstr) &&
		    (uc->compare_ino ? entry->ino != new_entry->ino : true)) {
			*badflags |= UNICRASH_NOT_UNIQUE;
			*existing_entry = entry;
			return true;
		}
		entry = entry->next;
	}

	return true;
}

/* Check a name for unicode normalization problems or collisions. */
static bool
__unicrash_check_name(
	struct unicrash		*uc,
	const char		*descr,
	const char		*namedescr,
	const char		*name,
	xfs_ino_t		ino)
{
	struct name_entry	*dup_entry = NULL;
	struct name_entry	*new_entry;
	unsigned int		badflags = 0;
	bool			moveon;

	/* If we can't create entry data, just skip it. */
	if (!name_entry_create(uc, name, ino, &new_entry))
		return true;

	name_entry_examine(new_entry, &badflags);

	moveon = unicrash_add(uc, new_entry, &badflags, &dup_entry);
	if (!moveon)
		return false;

	if (badflags)
		unicrash_complain(uc, descr, namedescr, new_entry, badflags,
				dup_entry);

	return true;
}

/* Check a directory entry for unicode normalization problems or collisions. */
bool
unicrash_check_dir_name(
	struct unicrash		*uc,
	const char		*descr,
	struct dirent		*dentry)
{
	if (!uc)
		return true;
	return __unicrash_check_name(uc, descr, _("directory"),
			dentry->d_name, dentry->d_ino);
}

/*
 * Check an extended attribute name for unicode normalization problems
 * or collisions.
 */
bool
unicrash_check_xattr_name(
	struct unicrash		*uc,
	const char		*descr,
	const char		*attrname)
{
	if (!uc)
		return true;
	return __unicrash_check_name(uc, descr, _("extended attribute"),
			attrname, 0);
}
