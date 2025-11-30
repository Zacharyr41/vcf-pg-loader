"""
Tests for variant normalization using vt test suite.

Test cases sourced from:
    vt (https://github.com/atks/vt)
    Copyright (c) 2013 Adrian Tan <atks@umich.edu>
    MIT License

Test data location: vt/test/normalize/
Reference: vt/test/ref/20.fa.gz

The vt normalization algorithm is described in:
    Tan A, Abecasis GR, Kang HM. Unified representation of genetic variants.
    Bioinformatics. 2015;31(13):2202-2204. doi:10.1093/bioinformatics/btv112
"""

import pytest

from vcf_pg_loader.normalizer import normalize_variant


class MockReferenceGenome:
    """
    Mock reference genome for chr20 based on vt test reference.

    This implements just enough of the reference sequence to support
    the vt test cases. In a real implementation, this would use pysam
    or similar to access the actual reference FASTA.
    """

    def __init__(self, sequences: dict[str, str]):
        self.sequences = sequences

    def fetch(self, chrom: str, start: int, end: int) -> str:
        """Fetch reference sequence for a region (0-based coordinates)."""
        key = f"{chrom}:{start}"
        if key in self.sequences:
            return self.sequences[key]
        return "N"


VT_TEST_CASES = [
    {
        "input": {"chrom": "20", "pos": 421808, "ref": "A", "alt": "ACCA"},
        "expected": {"pos": 421805, "ref": "T", "alt": "TCCA"},
        "ref_context": {"20:421804": "T"},
        "note": "Left-align insertion by 3bp",
    },
    {
        "input": {"chrom": "20", "pos": 1292033, "ref": "C", "alt": "CTTGT"},
        "expected": {"pos": 1292033, "ref": "C", "alt": "CTTGT"},
        "ref_context": {},
        "note": "Already normalized - no change",
    },
    {
        "input": {"chrom": "20", "pos": 1340527, "ref": "T", "alt": "TGTC"},
        "expected": {"pos": 1340527, "ref": "T", "alt": "TGTC"},
        "ref_context": {},
        "note": "Already normalized - no change",
    },
    {
        "input": {"chrom": "20", "pos": 1600125, "ref": "GAA", "alt": "G"},
        "expected": {"pos": 1600125, "ref": "GAA", "alt": "G"},
        "ref_context": {},
        "note": "Already normalized deletion",
    },
    {
        "input": {"chrom": "20", "pos": 2171404, "ref": "A", "alt": "AA"},
        "expected": {"pos": 2171402, "ref": "T", "alt": "TA"},
        "ref_context": {"20:2171401": "T"},
        "note": "Left-align A insertion by 2bp",
    },
    {
        "input": {"chrom": "20", "pos": 3373441, "ref": "TCTTT", "alt": "T"},
        "expected": {"pos": 3373437, "ref": "GCTTT", "alt": "G"},
        "ref_context": {"20:3373436": "G"},
        "note": "Left-align CTTT deletion by 4bp",
    },
    {
        "input": {"chrom": "20", "pos": 3635159, "ref": "T", "alt": "TT"},
        "expected": {"pos": 3635158, "ref": "A", "alt": "AT"},
        "ref_context": {"20:3635157": "A"},
        "note": "Left-align T insertion by 1bp",
    },
    {
        "input": {"chrom": "20", "pos": 4422119, "ref": "GCTCCCAGGCTACAGAAAGATGATGGAG", "alt": "G"},
        "expected": {"pos": 4422115, "ref": "GGGAGCTCCCAGGCTACAGAAAGATGAT", "alt": "G"},
        "ref_context": {"20:4422114": "G"},
        "note": "Left-align large deletion - rotates sequence",
    },
    {
        "input": {"chrom": "20", "pos": 5900670, "ref": "C", "alt": "CC"},
        "expected": {"pos": 5900669, "ref": "G", "alt": "GC"},
        "ref_context": {"20:5900668": "G"},
        "note": "Left-align C insertion by 1bp",
    },
    {
        "input": {"chrom": "20", "pos": 8080280, "ref": "GTTTG", "alt": "G"},
        "expected": {"pos": 8080272, "ref": "CTTTG", "alt": "C"},
        "ref_context": {"20:8080271": "C"},
        "note": "Left-align TTTG deletion by 8bp",
    },
    {
        "input": {"chrom": "20", "pos": 8781394, "ref": "AA", "alt": "A"},
        "expected": {"pos": 8781391, "ref": "CA", "alt": "C"},
        "ref_context": {"20:8781390": "C"},
        "note": "Left-align A deletion by 3bp",
    },
    {
        "input": {"chrom": "20", "pos": 8833756, "ref": "TT", "alt": "T"},
        "expected": {"pos": 8833755, "ref": "CT", "alt": "C"},
        "ref_context": {"20:8833754": "C"},
        "note": "Left-align T deletion by 1bp",
    },
    {
        "input": {"chrom": "20", "pos": 9035330, "ref": "T", "alt": "TT"},
        "expected": {"pos": 9035326, "ref": "A", "alt": "AT"},
        "ref_context": {"20:9035325": "A"},
        "note": "Left-align T insertion by 4bp",
    },
    {
        "input": {"chrom": "20", "pos": 12611892, "ref": "TCAAGT", "alt": "T"},
        "expected": {"pos": 12611888, "ref": "TAAGTC", "alt": "T"},
        "ref_context": {"20:12611887": "T"},
        "note": "Left-align CAAGT deletion - rotates sequence",
    },
    {
        "input": {"chrom": "20", "pos": 13527241, "ref": "T", "alt": "TTGT"},
        "expected": {"pos": 13527237, "ref": "C", "alt": "CTTG"},
        "ref_context": {"20:13527236": "C"},
        "note": "Left-align TGT insertion by 4bp - becomes TTG",
    },
    {
        "input": {"chrom": "20", "pos": 14923414, "ref": "CTAAAAGCC", "alt": "C"},
        "expected": {"pos": 14923410, "ref": "GAGCCTAAA", "alt": "G"},
        "ref_context": {"20:14923409": "G"},
        "note": "Left-align deletion - rotates sequence",
    },
    {
        "input": {"chrom": "20", "pos": 15082253, "ref": "GGTGG", "alt": "G"},
        "expected": {"pos": 15082252, "ref": "TGGTG", "alt": "T"},
        "ref_context": {"20:15082251": "T"},
        "note": "Left-align GTGG deletion by 1bp",
    },
    {
        "input": {"chrom": "20", "pos": 15701890, "ref": "TGAAGTCT", "alt": "T"},
        "expected": {"pos": 15701887, "ref": "ATCTGAAG", "alt": "A"},
        "ref_context": {"20:15701886": "A"},
        "note": "Left-align deletion - normalizes to same as 15701887",
    },
    {
        "input": {"chrom": "20", "pos": 17660514, "ref": "CTCTCCTAAACCC", "alt": "C"},
        "expected": {"pos": 17660506, "ref": "TCTAAACCCTCTC", "alt": "T"},
        "ref_context": {"20:17660505": "T"},
        "note": "Left-align deletion by 8bp - rotates sequence",
    },
    {
        "input": {"chrom": "20", "pos": 18487147, "ref": "AATAA", "alt": "A"},
        "expected": {"pos": 18487146, "ref": "GAATA", "alt": "G"},
        "ref_context": {"20:18487145": "G"},
        "note": "Left-align ATAA deletion by 1bp",
    },
    {
        "input": {"chrom": "20", "pos": 18624883, "ref": "TCT", "alt": "T"},
        "expected": {"pos": 18624882, "ref": "ATC", "alt": "A"},
        "ref_context": {"20:18624881": "A"},
        "note": "Left-align CT deletion by 1bp - becomes TC",
    },
    {
        "input": {"chrom": "20", "pos": 19076746, "ref": "CTCCCC", "alt": "C"},
        "expected": {"pos": 19076743, "ref": "ACCCTC", "alt": "A"},
        "ref_context": {"20:19076742": "A"},
        "note": "Left-align TCCCC deletion by 3bp",
    },
    {
        "input": {"chrom": "20", "pos": 20015197, "ref": "A", "alt": "AAACA"},
        "expected": {"pos": 20015192, "ref": "T", "alt": "TAAAC"},
        "ref_context": {"20:20015191": "T"},
        "note": "Left-align AACA insertion by 5bp",
    },
    {
        "input": {"chrom": "20", "pos": 20015650, "ref": "CTC", "alt": "C"},
        "expected": {"pos": 20015647, "ref": "ACT", "alt": "A"},
        "ref_context": {"20:20015646": "A"},
        "note": "Left-align TC deletion by 3bp",
    },
    {
        "input": {"chrom": "20", "pos": 20033568, "ref": "T", "alt": "TAATT"},
        "expected": {"pos": 20033563, "ref": "A", "alt": "ATAAT"},
        "ref_context": {"20:20033562": "A"},
        "note": "Left-align AATT insertion by 5bp",
    },
    {
        "input": {"chrom": "20", "pos": 20158176, "ref": "TT", "alt": "T"},
        "expected": {"pos": 20158175, "ref": "CT", "alt": "C"},
        "ref_context": {"20:20158174": "C"},
        "note": "Left-align T deletion by 1bp",
    },
    {
        "input": {"chrom": "20", "pos": 20978417, "ref": "T", "alt": "TCT"},
        "expected": {"pos": 20978414, "ref": "A", "alt": "ATC"},
        "ref_context": {"20:20978413": "A"},
        "note": "Left-align CT insertion by 3bp - becomes TC",
    },
    {
        "input": {"chrom": "20", "pos": 21383368, "ref": "TACT", "alt": "T"},
        "expected": {"pos": 21383366, "ref": "TCTA", "alt": "T"},
        "ref_context": {"20:21383365": "T"},
        "note": "Left-align ACT deletion by 2bp - becomes CTA",
    },
    {
        "input": {"chrom": "20", "pos": 21855322, "ref": "T", "alt": "TT"},
        "expected": {"pos": 21855321, "ref": "C", "alt": "CT"},
        "ref_context": {"20:21855320": "C"},
        "note": "Left-align T insertion by 1bp",
    },
    {
        "input": {"chrom": "20", "pos": 21864933, "ref": "GTAATG", "alt": "G"},
        "expected": {"pos": 21864931, "ref": "CTGTAA", "alt": "C"},
        "ref_context": {"20:21864930": "C"},
        "note": "Left-align TAATG deletion by 2bp",
    },
    {
        "input": {"chrom": "20", "pos": 22088984, "ref": "G", "alt": "GG"},
        "expected": {"pos": 22088982, "ref": "T", "alt": "TG"},
        "ref_context": {"20:22088981": "T"},
        "note": "Left-align G insertion by 2bp",
    },
    {
        "input": {"chrom": "20", "pos": 22449570, "ref": "C", "alt": "CCTC"},
        "expected": {"pos": 22449567, "ref": "T", "alt": "TCTC"},
        "ref_context": {"20:22449566": "T"},
        "note": "Left-align CTC insertion by 3bp",
    },
    {
        "input": {"chrom": "20", "pos": 22703265, "ref": "G", "alt": "GAAGG"},
        "expected": {"pos": 22703260, "ref": "A", "alt": "AGAAG"},
        "ref_context": {"20:22703259": "A"},
        "note": "Left-align AAGG insertion by 5bp - becomes GAAG",
    },
    {
        "input": {"chrom": "20", "pos": 22984189, "ref": "AA", "alt": "A"},
        "expected": {"pos": 22984188, "ref": "CA", "alt": "C"},
        "ref_context": {"20:22984187": "C"},
        "note": "Left-align A deletion by 1bp",
    },
    {
        "input": {"chrom": "20", "pos": 24375706, "ref": "CAGGATGC", "alt": "C"},
        "expected": {"pos": 24375698, "ref": "CCAGGATG", "alt": "C"},
        "ref_context": {"20:24375697": "C"},
        "note": "Left-align AGGATGC deletion by 8bp",
    },
    {
        "input": {"chrom": "20", "pos": 29900012, "ref": "GAG", "alt": "G"},
        "expected": {"pos": 29900010, "ref": "TAG", "alt": "T"},
        "ref_context": {"20:29900009": "T"},
        "note": "Left-align AG deletion by 2bp",
    },
    {
        "input": {"chrom": "20", "pos": 30110429, "ref": "GTG", "alt": "G"},
        "expected": {"pos": 30110427, "ref": "CTG", "alt": "C"},
        "ref_context": {"20:30110426": "C"},
        "note": "Left-align TG deletion by 2bp",
    },
    {
        "input": {"chrom": "20", "pos": 30747545, "ref": "TATT", "alt": "T"},
        "expected": {"pos": 30747542, "ref": "CATT", "alt": "C"},
        "ref_context": {"20:30747541": "C"},
        "note": "Left-align ATT deletion by 3bp",
    },
    {
        "input": {"chrom": "20", "pos": 32871422, "ref": "GG", "alt": "G"},
        "expected": {"pos": 32871421, "ref": "AG", "alt": "A"},
        "ref_context": {"20:32871420": "A"},
        "note": "Left-align G deletion by 1bp",
    },
    {
        "input": {"chrom": "20", "pos": 33626401, "ref": "G", "alt": "GAAG"},
        "expected": {"pos": 33626399, "ref": "C", "alt": "CAGA"},
        "ref_context": {"20:33626398": "C"},
        "note": "Left-align AAG insertion by 2bp - becomes AGA",
    },
    {
        "input": {"chrom": "20", "pos": 35525288, "ref": "G", "alt": "GGACAG"},
        "expected": {"pos": 35525287, "ref": "T", "alt": "TGGACA"},
        "ref_context": {"20:35525286": "T"},
        "note": "Left-align GACAG insertion by 1bp - becomes GGACA",
    },
    {
        "input": {"chrom": "20", "pos": 36010629, "ref": "G", "alt": "GCAGGGTG"},
        "expected": {"pos": 36010625, "ref": "A", "alt": "AGGTGCAG"},
        "ref_context": {"20:36010624": "A"},
        "note": "Left-align CAGGGTG insertion by 4bp",
    },
    {
        "input": {"chrom": "20", "pos": 36686811, "ref": "TTT", "alt": "T"},
        "expected": {"pos": 36686810, "ref": "CTT", "alt": "C"},
        "ref_context": {"20:36686809": "C"},
        "note": "Left-align TT deletion by 1bp",
    },
    {
        "input": {"chrom": "20", "pos": 37048678, "ref": "A", "alt": "ATA"},
        "expected": {"pos": 37048675, "ref": "A", "alt": "AAT"},
        "ref_context": {"20:37048674": "A"},
        "note": "Left-align TA insertion by 3bp - becomes AT",
    },
    {
        "input": {"chrom": "20", "pos": 37388010, "ref": "ACTCTAGCGAGA", "alt": "A"},
        "expected": {"pos": 37388009, "ref": "AACTCTAGCGAG", "alt": "A"},
        "ref_context": {"20:37388008": "A"},
        "note": "Left-align CTCTAGCGAGA deletion by 1bp",
    },
    {
        "input": {"chrom": "20", "pos": 37394796, "ref": "TT", "alt": "T"},
        "expected": {"pos": 37394795, "ref": "AT", "alt": "A"},
        "ref_context": {"20:37394794": "A"},
        "note": "Left-align T deletion by 1bp",
    },
    {
        "input": {"chrom": "20", "pos": 38022703, "ref": "GAAG", "alt": "G"},
        "expected": {"pos": 38022700, "ref": "CAAG", "alt": "C"},
        "ref_context": {"20:38022699": "C"},
        "note": "Left-align AAG deletion by 3bp",
    },
    {
        "input": {"chrom": "20", "pos": 38416239, "ref": "TTGAT", "alt": "T"},
        "expected": {"pos": 38416231, "ref": "CTGAT", "alt": "C"},
        "ref_context": {"20:38416230": "C"},
        "note": "Left-align TGAT deletion by 8bp",
    },
    {
        "input": {"chrom": "20", "pos": 38441280, "ref": "AA", "alt": "A"},
        "expected": {"pos": 38441276, "ref": "TA", "alt": "T"},
        "ref_context": {"20:38441275": "T"},
        "note": "Left-align A deletion by 4bp",
    },
    {
        "input": {"chrom": "20", "pos": 39074861, "ref": "TCT", "alt": "T"},
        "expected": {"pos": 39074860, "ref": "TTC", "alt": "T"},
        "ref_context": {"20:39074859": "T"},
        "note": "Left-align CT deletion by 1bp - becomes TC",
    },
    {
        "input": {"chrom": "20", "pos": 39918080, "ref": "AGA", "alt": "A"},
        "expected": {"pos": 39918077, "ref": "CAG", "alt": "C"},
        "ref_context": {"20:39918076": "C"},
        "note": "Left-align GA deletion by 3bp - becomes AG",
    },
    {
        "input": {"chrom": "20", "pos": 41086660, "ref": "AA", "alt": "A"},
        "expected": {"pos": 41086658, "ref": "CA", "alt": "C"},
        "ref_context": {"20:41086657": "C"},
        "note": "Left-align A deletion by 2bp",
    },
    {
        "input": {"chrom": "20", "pos": 41284377, "ref": "T", "alt": "TTT"},
        "expected": {"pos": 41284374, "ref": "G", "alt": "GTT"},
        "ref_context": {"20:41284373": "G"},
        "note": "Left-align TT insertion by 3bp",
    },
    {
        "input": {"chrom": "20", "pos": 42551669, "ref": "G", "alt": "GAG"},
        "expected": {"pos": 42551667, "ref": "C", "alt": "CAG"},
        "ref_context": {"20:42551666": "C"},
        "note": "Left-align AG insertion by 2bp",
    },
    {
        "input": {"chrom": "20", "pos": 42595607, "ref": "ACCAACA", "alt": "A"},
        "expected": {"pos": 42595599, "ref": "CCACCAA", "alt": "C"},
        "ref_context": {"20:42595598": "C"},
        "note": "Left-align CCAACA deletion by 8bp",
    },
    {
        "input": {"chrom": "20", "pos": 43521315, "ref": "ACTCTA", "alt": "A"},
        "expected": {"pos": 43521313, "ref": "TTACTC", "alt": "T"},
        "ref_context": {"20:43521312": "T"},
        "note": "Left-align CTCTA deletion by 2bp",
    },
    {
        "input": {"chrom": "20", "pos": 45850737, "ref": "GAG", "alt": "G"},
        "expected": {"pos": 45850735, "ref": "TAG", "alt": "T"},
        "ref_context": {"20:45850734": "T"},
        "note": "Left-align AG deletion by 2bp",
    },
    {
        "input": {"chrom": "20", "pos": 45924828, "ref": "TT", "alt": "T"},
        "expected": {"pos": 45924827, "ref": "CT", "alt": "C"},
        "ref_context": {"20:45924826": "C"},
        "note": "Left-align T deletion by 1bp",
    },
    {
        "input": {"chrom": "20", "pos": 46657104, "ref": "C", "alt": "CTCC"},
        "expected": {"pos": 46657103, "ref": "A", "alt": "ACTC"},
        "ref_context": {"20:46657102": "A"},
        "note": "Left-align TCC insertion by 1bp - becomes CTC",
    },
    {
        "input": {"chrom": "20", "pos": 46687123, "ref": "T", "alt": "TAT"},
        "expected": {"pos": 46687122, "ref": "T", "alt": "TTA"},
        "ref_context": {"20:46687121": "T"},
        "note": "Left-align AT insertion by 1bp - becomes TA",
    },
    {
        "input": {"chrom": "20", "pos": 46981904, "ref": "ATGAGCTCTAAA", "alt": "A"},
        "expected": {"pos": 46981903, "ref": "GATGAGCTCTAA", "alt": "G"},
        "ref_context": {"20:46981902": "G"},
        "note": "Left-align TGAGCTCTAAA deletion by 1bp",
    },
    {
        "input": {"chrom": "20", "pos": 48696636, "ref": "G", "alt": "GG"},
        "expected": {"pos": 48696635, "ref": "A", "alt": "AG"},
        "ref_context": {"20:48696634": "A"},
        "note": "Left-align G insertion by 1bp",
    },
    {
        "input": {"chrom": "20", "pos": 49350131, "ref": "A", "alt": "ATA"},
        "expected": {"pos": 49350127, "ref": "C", "alt": "CTA"},
        "ref_context": {"20:49350126": "C"},
        "note": "Left-align TA insertion by 4bp",
    },
    {
        "input": {"chrom": "20", "pos": 50018388, "ref": "GGTGTCAACAAATAG", "alt": "G"},
        "expected": {"pos": 50018386, "ref": "AAGGTGTCAACAAAT", "alt": "A"},
        "ref_context": {"20:50018385": "A"},
        "note": "Left-align large deletion by 2bp",
    },
    {
        "input": {"chrom": "20", "pos": 50114687, "ref": "TT", "alt": "T"},
        "expected": {"pos": 50114685, "ref": "CT", "alt": "C"},
        "ref_context": {"20:50114684": "C"},
        "note": "Left-align T deletion by 2bp",
    },
    {
        "input": {"chrom": "20", "pos": 50712021, "ref": "C", "alt": "CTC"},
        "expected": {"pos": 50712019, "ref": "G", "alt": "GTC"},
        "ref_context": {"20:50712018": "G"},
        "note": "Left-align TC insertion by 2bp",
    },
    {
        "input": {"chrom": "20", "pos": 51512776, "ref": "TCT", "alt": "T"},
        "expected": {"pos": 51512772, "ref": "GCT", "alt": "G"},
        "ref_context": {"20:51512771": "G"},
        "note": "Left-align CT deletion by 4bp",
    },
    {
        "input": {"chrom": "20", "pos": 51731206, "ref": "C", "alt": "CC"},
        "expected": {"pos": 51731202, "ref": "T", "alt": "TC"},
        "ref_context": {"20:51731201": "T"},
        "note": "Left-align C insertion by 4bp",
    },
    {
        "input": {"chrom": "20", "pos": 52772453, "ref": "A", "alt": "AA"},
        "expected": {"pos": 52772452, "ref": "T", "alt": "TA"},
        "ref_context": {"20:52772451": "T"},
        "note": "Left-align A insertion by 1bp",
    },
    {
        "input": {"chrom": "20", "pos": 54158733, "ref": "T", "alt": "TCTCT"},
        "expected": {"pos": 54158730, "ref": "G", "alt": "GTCTC"},
        "ref_context": {"20:54158729": "G"},
        "note": "Left-align CTCT insertion by 3bp",
    },
    {
        "input": {"chrom": "20", "pos": 54664038, "ref": "A", "alt": "AAA"},
        "expected": {"pos": 54664037, "ref": "T", "alt": "TAA"},
        "ref_context": {"20:54664036": "T"},
        "note": "Left-align AA insertion by 1bp",
    },
    {
        "input": {"chrom": "20", "pos": 55292358, "ref": "TTT", "alt": "T"},
        "expected": {"pos": 55292357, "ref": "GTT", "alt": "G"},
        "ref_context": {"20:55292356": "G"},
        "note": "Left-align TT deletion by 1bp",
    },
    {
        "input": {"chrom": "20", "pos": 55444416, "ref": "GGAG", "alt": "G"},
        "expected": {"pos": 55444413, "ref": "AGAG", "alt": "A"},
        "ref_context": {"20:55444412": "A"},
        "note": "Left-align GAG deletion by 3bp",
    },
    {
        "input": {"chrom": "20", "pos": 55557713, "ref": "AGCTCTGA", "alt": "A"},
        "expected": {"pos": 55557711, "ref": "AGAGCTCT", "alt": "A"},
        "ref_context": {"20:55557710": "A"},
        "note": "Left-align GCTCTGA deletion by 2bp",
    },
    {
        "input": {"chrom": "20", "pos": 56311463, "ref": "T", "alt": "TT"},
        "expected": {"pos": 56311461, "ref": "A", "alt": "AT"},
        "ref_context": {"20:56311460": "A"},
        "note": "Left-align T insertion by 2bp",
    },
    {
        "input": {"chrom": "20", "pos": 56788032, "ref": "T", "alt": "TAT"},
        "expected": {"pos": 56788029, "ref": "T", "alt": "TTA"},
        "ref_context": {"20:56788028": "T"},
        "note": "Left-align AT insertion by 3bp - becomes TA",
    },
    {
        "input": {"chrom": "20", "pos": 57863967, "ref": "A", "alt": "AAACCA"},
        "expected": {"pos": 57863961, "ref": "G", "alt": "GAAACC"},
        "ref_context": {"20:57863960": "G"},
        "note": "Left-align AACCA insertion by 6bp",
    },
    {
        "input": {"chrom": "20", "pos": 57967774, "ref": "T", "alt": "TT"},
        "expected": {"pos": 57967772, "ref": "A", "alt": "AT"},
        "ref_context": {"20:57967771": "A"},
        "note": "Left-align T insertion by 2bp",
    },
    {
        "input": {"chrom": "20", "pos": 58742718, "ref": "TACTGAT", "alt": "T"},
        "expected": {"pos": 58742717, "ref": "CTACTGA", "alt": "C"},
        "ref_context": {"20:58742716": "C"},
        "note": "Left-align ACTGAT deletion by 1bp",
    },
    {
        "input": {"chrom": "20", "pos": 59705557, "ref": "TTTGT", "alt": "T"},
        "expected": {"pos": 59705552, "ref": "CTTTG", "alt": "C"},
        "ref_context": {"20:59705551": "C"},
        "note": "Left-align TTGT deletion by 5bp",
    },
    {
        "input": {"chrom": "20", "pos": 59917975, "ref": "A", "alt": "ATTA"},
        "expected": {"pos": 59917971, "ref": "C", "alt": "CATT"},
        "ref_context": {"20:59917970": "C"},
        "note": "Left-align TTA insertion by 4bp - becomes ATT",
    },
    {
        "input": {"chrom": "20", "pos": 59961560, "ref": "TTT", "alt": "T"},
        "expected": {"pos": 59961559, "ref": "CTT", "alt": "C"},
        "ref_context": {"20:59961558": "C"},
        "note": "Left-align TT deletion by 1bp",
    },
    {
        "input": {"chrom": "20", "pos": 60744904, "ref": "ACCGTCCACA", "alt": "A"},
        "expected": {"pos": 60744897, "ref": "TGTCCACACC", "alt": "T"},
        "ref_context": {"20:60744896": "T"},
        "note": "Left-align large deletion by 7bp - sequence rotates",
    },
]


class TestVtNormalization:
    """
    Test normalization using cases from vt test suite.

    Source: vt/test/normalize/01_IN.vcf -> 01_OUT.vcf
    Reference: vt/test/ref/20.fa.gz

    These test cases validate that our normalization implementation
    produces the same results as the vt tool.
    """

    @pytest.mark.parametrize("test_case", VT_TEST_CASES, ids=lambda tc: f"{tc['input']['pos']}_{tc['note'][:30]}")
    def test_vt_normalization_case(self, test_case):
        """Test normalization against vt expected output."""
        inp = test_case["input"]
        exp = test_case["expected"]
        ref_context = test_case["ref_context"]

        ref_genome = MockReferenceGenome(ref_context)

        result_pos, result_ref, result_alts = normalize_variant(
            inp["chrom"],
            inp["pos"],
            inp["ref"],
            [inp["alt"]],
            ref_genome
        )

        assert result_pos == exp["pos"], f"Position mismatch for {test_case['note']}: expected {exp['pos']}, got {result_pos}"
        assert result_ref == exp["ref"], f"REF mismatch for {test_case['note']}: expected {exp['ref']}, got {result_ref}"
        assert result_alts[0] == exp["alt"], f"ALT mismatch for {test_case['note']}: expected {exp['alt']}, got {result_alts[0]}"


class TestVtNormalizationNoChange:
    """Test cases where vt does not change the variant (already normalized)."""

    NO_CHANGE_CASES = [
        {"chrom": "20", "pos": 1292033, "ref": "C", "alt": "CTTGT"},
        {"chrom": "20", "pos": 1340527, "ref": "T", "alt": "TGTC"},
        {"chrom": "20", "pos": 1600125, "ref": "GAA", "alt": "G"},
        {"chrom": "20", "pos": 1728298, "ref": "G", "alt": "GT"},
        {"chrom": "20", "pos": 2171402, "ref": "T", "alt": "TA"},
        {"chrom": "20", "pos": 5280839, "ref": "T", "alt": "TATA"},
        {"chrom": "20", "pos": 5291223, "ref": "TCAG", "alt": "T"},
        {"chrom": "20", "pos": 6351757, "ref": "C", "alt": "CTT"},
        {"chrom": "20", "pos": 9311904, "ref": "TGTATCTGTCCA", "alt": "T"},
    ]

    @pytest.mark.parametrize("case", NO_CHANGE_CASES, ids=lambda c: str(c["pos"]))
    def test_already_normalized(self, case):
        """Test that already-normalized variants are unchanged."""
        result_pos, result_ref, result_alts = normalize_variant(
            case["chrom"],
            case["pos"],
            case["ref"],
            [case["alt"]],
            None
        )

        assert result_pos == case["pos"]
        assert result_ref == case["ref"]
        assert result_alts[0] == case["alt"]
