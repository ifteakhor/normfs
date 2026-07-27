#include "normfs/crc32c.h"

/*
 * Frama-C sees the fast path's contract but not its body: the intrinsics have
 * no meaning it can interpret. The contract is therefore assumed, and the
 * dispatcher below is proven against it, so what WP establishes is the code
 * that actually runs rather than an unused portable twin. The assumption is
 * exactly "the CRC32 instruction computes CRC32C as its ISA specifies", and
 * test_crc32c.c checks it against the proven table path on every build.
 */
#if defined(__FRAMAC__)
#define NORMFS_CRC32C_HW 1
#elif !defined(NORMFS_CRC32C_PORTABLE_ONLY) && \
    defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__

#if defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
#define NORMFS_CRC32C_ARM 1
#define NORMFS_CRC32C_HW 1
#define NORMFS_CRC32C_HW_TARGET
#define NORMFS_CRC32C_STEP8(c, w) __crc32cd((c), (w))
#define NORMFS_CRC32C_STEP1(c, b) __crc32cb((c), (b))
#include <arm_acle.h>
#include <string.h>
/*
 * CRC32 is optional in ARMv8.0 and only mandatory from ARMv8.1, so a build for
 * generic armv8-a leaves __ARM_FEATURE_CRC32 undefined and would otherwise
 * silently take the table path on hardware that has the instruction. Compile
 * the fast path for +crc anyway and select it from AT_HWCAP at runtime.
 */
#elif defined(__aarch64__) && defined(__linux__) && \
    (defined(__clang__) || (defined(__GNUC__) && __GNUC__ >= 11))
#define NORMFS_CRC32C_ARM 1
#define NORMFS_CRC32C_ARM_RUNTIME 1
#define NORMFS_CRC32C_HW 1
#define NORMFS_CRC32C_HW_TARGET __attribute__((target("+crc")))
#define NORMFS_CRC32C_STEP8(c, w) __crc32cd((c), (w))
#define NORMFS_CRC32C_STEP1(c, b) __crc32cb((c), (b))
#include <arm_acle.h>
#include <asm/hwcap.h>
#include <string.h>
#include <sys/auxv.h>
#elif defined(__x86_64__) && (defined(__GNUC__) || defined(__clang__))
#define NORMFS_CRC32C_X86 1
#define NORMFS_CRC32C_HW 1
#define NORMFS_CRC32C_HW_TARGET __attribute__((target("sse4.2")))
#define NORMFS_CRC32C_STEP8(c, w) ((uint32_t)_mm_crc32_u64((uint64_t)(c), (w)))
#define NORMFS_CRC32C_STEP1(c, b) ((uint32_t)_mm_crc32_u8((c), (b)))
#include <nmmintrin.h>
#include <string.h>
#endif

#endif

#if defined(NORMFS_CRC32C_HW)

/* Zero until the constructor below runs, so a call from another translation
 * unit's constructor takes the portable path rather than an absent one. */
extern int normfs_crc32c_hw_supported;

/*@ requires len == 0 || \valid_read(data + (0 .. len - 1));
    assigns \nothing;
    ensures \result == normfs_crc32c_logic(crc, data, len);
*/
uint32_t normfs_crc32c_hw(uint32_t crc, const uint8_t *data, size_t len);

#endif

const uint32_t normfs_crc32c_table[256] = {
	0x00000000U, 0xF26B8303U, 0xE13B70F7U, 0x1350F3F4U,
	0xC79A971FU, 0x35F1141CU, 0x26A1E7E8U, 0xD4CA64EBU,
	0x8AD958CFU, 0x78B2DBCCU, 0x6BE22838U, 0x9989AB3BU,
	0x4D43CFD0U, 0xBF284CD3U, 0xAC78BF27U, 0x5E133C24U,
	0x105EC76FU, 0xE235446CU, 0xF165B798U, 0x030E349BU,
	0xD7C45070U, 0x25AFD373U, 0x36FF2087U, 0xC494A384U,
	0x9A879FA0U, 0x68EC1CA3U, 0x7BBCEF57U, 0x89D76C54U,
	0x5D1D08BFU, 0xAF768BBCU, 0xBC267848U, 0x4E4DFB4BU,
	0x20BD8EDEU, 0xD2D60DDDU, 0xC186FE29U, 0x33ED7D2AU,
	0xE72719C1U, 0x154C9AC2U, 0x061C6936U, 0xF477EA35U,
	0xAA64D611U, 0x580F5512U, 0x4B5FA6E6U, 0xB93425E5U,
	0x6DFE410EU, 0x9F95C20DU, 0x8CC531F9U, 0x7EAEB2FAU,
	0x30E349B1U, 0xC288CAB2U, 0xD1D83946U, 0x23B3BA45U,
	0xF779DEAEU, 0x05125DADU, 0x1642AE59U, 0xE4292D5AU,
	0xBA3A117EU, 0x4851927DU, 0x5B016189U, 0xA96AE28AU,
	0x7DA08661U, 0x8FCB0562U, 0x9C9BF696U, 0x6EF07595U,
	0x417B1DBCU, 0xB3109EBFU, 0xA0406D4BU, 0x522BEE48U,
	0x86E18AA3U, 0x748A09A0U, 0x67DAFA54U, 0x95B17957U,
	0xCBA24573U, 0x39C9C670U, 0x2A993584U, 0xD8F2B687U,
	0x0C38D26CU, 0xFE53516FU, 0xED03A29BU, 0x1F682198U,
	0x5125DAD3U, 0xA34E59D0U, 0xB01EAA24U, 0x42752927U,
	0x96BF4DCCU, 0x64D4CECFU, 0x77843D3BU, 0x85EFBE38U,
	0xDBFC821CU, 0x2997011FU, 0x3AC7F2EBU, 0xC8AC71E8U,
	0x1C661503U, 0xEE0D9600U, 0xFD5D65F4U, 0x0F36E6F7U,
	0x61C69362U, 0x93AD1061U, 0x80FDE395U, 0x72966096U,
	0xA65C047DU, 0x5437877EU, 0x4767748AU, 0xB50CF789U,
	0xEB1FCBADU, 0x197448AEU, 0x0A24BB5AU, 0xF84F3859U,
	0x2C855CB2U, 0xDEEEDFB1U, 0xCDBE2C45U, 0x3FD5AF46U,
	0x7198540DU, 0x83F3D70EU, 0x90A324FAU, 0x62C8A7F9U,
	0xB602C312U, 0x44694011U, 0x5739B3E5U, 0xA55230E6U,
	0xFB410CC2U, 0x092A8FC1U, 0x1A7A7C35U, 0xE811FF36U,
	0x3CDB9BDDU, 0xCEB018DEU, 0xDDE0EB2AU, 0x2F8B6829U,
	0x82F63B78U, 0x709DB87BU, 0x63CD4B8FU, 0x91A6C88CU,
	0x456CAC67U, 0xB7072F64U, 0xA457DC90U, 0x563C5F93U,
	0x082F63B7U, 0xFA44E0B4U, 0xE9141340U, 0x1B7F9043U,
	0xCFB5F4A8U, 0x3DDE77ABU, 0x2E8E845FU, 0xDCE5075CU,
	0x92A8FC17U, 0x60C37F14U, 0x73938CE0U, 0x81F80FE3U,
	0x55326B08U, 0xA759E80BU, 0xB4091BFFU, 0x466298FCU,
	0x1871A4D8U, 0xEA1A27DBU, 0xF94AD42FU, 0x0B21572CU,
	0xDFEB33C7U, 0x2D80B0C4U, 0x3ED04330U, 0xCCBBC033U,
	0xA24BB5A6U, 0x502036A5U, 0x4370C551U, 0xB11B4652U,
	0x65D122B9U, 0x97BAA1BAU, 0x84EA524EU, 0x7681D14DU,
	0x2892ED69U, 0xDAF96E6AU, 0xC9A99D9EU, 0x3BC21E9DU,
	0xEF087A76U, 0x1D63F975U, 0x0E330A81U, 0xFC588982U,
	0xB21572C9U, 0x407EF1CAU, 0x532E023EU, 0xA145813DU,
	0x758FE5D6U, 0x87E466D5U, 0x94B49521U, 0x66DF1622U,
	0x38CC2A06U, 0xCAA7A905U, 0xD9F75AF1U, 0x2B9CD9F2U,
	0xFF56BD19U, 0x0D3D3E1AU, 0x1E6DCDEEU, 0xEC064EEDU,
	0xC38D26C4U, 0x31E6A5C7U, 0x22B65633U, 0xD0DDD530U,
	0x0417B1DBU, 0xF67C32D8U, 0xE52CC12CU, 0x1747422FU,
	0x49547E0BU, 0xBB3FFD08U, 0xA86F0EFCU, 0x5A048DFFU,
	0x8ECEE914U, 0x7CA56A17U, 0x6FF599E3U, 0x9D9E1AE0U,
	0xD3D3E1ABU, 0x21B862A8U, 0x32E8915CU, 0xC083125FU,
	0x144976B4U, 0xE622F5B7U, 0xF5720643U, 0x07198540U,
	0x590AB964U, 0xAB613A67U, 0xB831C993U, 0x4A5A4A90U,
	0x9E902E7BU, 0x6CFBAD78U, 0x7FAB5E8CU, 0x8DC0DD8FU,
	0xE330A81AU, 0x115B2B19U, 0x020BD8EDU, 0xF0605BEEU,
	0x24AA3F05U, 0xD6C1BC06U, 0xC5914FF2U, 0x37FACCF1U,
	0x69E9F0D5U, 0x9B8273D6U, 0x88D28022U, 0x7AB90321U,
	0xAE7367CAU, 0x5C18E4C9U, 0x4F48173DU, 0xBD23943EU,
	0xF36E6F75U, 0x0105EC76U, 0x12551F82U, 0xE03E9C81U,
	0x34F4F86AU, 0xC69F7B69U, 0xD5CF889DU, 0x27A40B9EU,
	0x79B737BAU, 0x8BDCB4B9U, 0x988C474DU, 0x6AE7C44EU,
	0xBE2DA0A5U, 0x4C4623A6U, 0x5F16D052U, 0xAD7D5351U
};

/*@ requires len == 0 || \valid_read(data + (0 .. len - 1));
    assigns \nothing;
    ensures \result == normfs_crc32c_logic(crc, data, len);
*/
uint32_t
normfs_crc32c_portable(uint32_t crc, const uint8_t *data, size_t len)
{
	uint32_t c = 0xFFFFFFFFu ^ crc;
	size_t i;

	/*@ loop invariant 0 <= i <= len;
	    loop invariant c == normfs_crc32c_fold{Pre}(0xFFFFFFFF ^ crc, data, i);
	    loop assigns i, c;
	    loop variant len - i;
	*/
	for (i = 0u; i < len; i++) {
		uint32_t idx = (c ^ (uint32_t)data[i]) & 0xFFu;
		uint32_t hi = c >> 8;

		/*@ assert normfs_crc32c_fold{Pre}(0xFFFFFFFF ^ crc, data, i + 1) ==
		             (normfs_crc32c_table[idx] ^ hi); */
		c = normfs_crc32c_table[idx] ^ hi;
	}

	return 0xFFFFFFFFu ^ c;
}

#if !defined(NORMFS_CRC32C_PORTABLE_ONLY)

/*
 * Columns of the GF(2) operator "append N zero bytes" to the raw CRC state:
 * normfs_crc32c_shift_N[i] is the state after folding N zero bytes from state
 * 1u << i (x^(8N) mod P applied to basis bit i, reflected convention, no
 * inversion). Used to join independently folded streams. Not part of the
 * public API; exported without a header declaration so test_crc32c can
 * re-derive them from the polynomial by repeated squaring and check them
 * entry by entry.
 */
const uint32_t normfs_crc32c_shift_256[32] = {
	0xDCB17AA4U, 0xBC8E83B9U, 0x7CF17183U, 0xF9E2E306U,
	0xF629B0FDU, 0xE9BF170BU, 0xD69258E7U, 0xA8C8C73FU,
	0x547DF88FU, 0xA8FBF11EU, 0x541B94CDU, 0xA837299AU,
	0x558225C5U, 0xAB044B8AU, 0x53E4E1E5U, 0xA7C9C3CAU,
	0x4A7FF165U, 0x94FFE2CAU, 0x2C13B365U, 0x582766CAU,
	0xB04ECD94U, 0x6571EDD9U, 0xCAE3DBB2U, 0x902BC195U,
	0x25BBF5DBU, 0x4B77EBB6U, 0x96EFD76CU, 0x2833D829U,
	0x5067B052U, 0xA0CF60A4U, 0x4472B7B9U, 0x88E56F72U
};

const uint32_t normfs_crc32c_shift_512[32] = {
	0xBD6F81F8U, 0x7F337501U, 0xFE66EA02U, 0xF921A2F5U,
	0xF7AF331BU, 0xEAB210C7U, 0xD088577FU, 0xA4FCD80FU,
	0x4C15C6EFU, 0x982B8DDEU, 0x35BB6D4DU, 0x6B76DA9AU,
	0xD6EDB534U, 0xA8371C99U, 0x55824FC3U, 0xAB049F86U,
	0x53E549FDU, 0xA7CA93FAU, 0x4A795105U, 0x94F2A20AU,
	0x2C0932E5U, 0x581265CAU, 0xB024CB94U, 0x65A5E1D9U,
	0xCB4BC3B2U, 0x937BF195U, 0x231B95DBU, 0x46372BB6U,
	0x8C6E576CU, 0x1D30D829U, 0x3A61B052U, 0x74C360A4U
};

const uint32_t normfs_crc32c_shift_1024[32] = {
	0xFE314258U, 0xF98EF241U, 0xF6F19273U, 0xE80F5217U,
	0xD5F2D2DFU, 0xAE09D34FU, 0x59FFD06FU, 0xB3FFA0DEU,
	0x6213374DU, 0xC4266E9AU, 0x8DA0ABC5U, 0x1EAD217BU,
	0x3D5A42F6U, 0x7AB485ECU, 0xF5690BD8U, 0xEF3E6141U,
	0xDB90B473U, 0xB2CD1E17U, 0x60764ADFU, 0xC0EC95BEU,
	0x84355D8DU, 0x0D86CDEBU, 0x1B0D9BD6U, 0x361B37ACU,
	0x6C366F58U, 0xD86CDEB0U, 0xB535CB91U, 0x6F87E1D3U,
	0xDF0FC3A6U, 0xBBF3F1BDU, 0x720B958BU, 0xE4172B16U
};

const uint32_t normfs_crc32c_shift_2048[32] = {
	0xF7506984U, 0xEB4CA5F9U, 0xD3753D03U, 0xA3060CF7U,
	0x43E06F1FU, 0x87C0DE3EU, 0x0A6DCA8DU, 0x14DB951AU,
	0x29B72A34U, 0x536E5468U, 0xA6DCA8D0U, 0x48552751U,
	0x90AA4EA2U, 0x24B8EBB5U, 0x4971D76AU, 0x92E3AED4U,
	0x202B2B59U, 0x405656B2U, 0x80ACAD64U, 0x04B52C39U,
	0x096A5872U, 0x12D4B0E4U, 0x25A961C8U, 0x4B52C390U,
	0x96A58720U, 0x28A778B1U, 0x514EF162U, 0xA29DE2C4U,
	0x40D7B379U, 0x81AF66F2U, 0x06B2BB15U, 0x0D65762AU
};

#endif

#if defined(NORMFS_CRC32C_HW) && !defined(__FRAMAC__)

/*
 * Image of v under the operator whose columns are m. Branchless: a bit-test
 * loop mispredicts about half the time on random states.
 */
static uint32_t
normfs_crc32c_shift(uint32_t v, const uint32_t m[32])
{
	uint32_t r = 0u;
	uint32_t i;

	for (i = 0u; i < 32u; i++)
		r ^= m[i] & (0u - ((v >> i) & 1u));

	return r;
}

/* Serial fold over the raw CRC state: no inversion on entry or exit. */
NORMFS_CRC32C_HW_TARGET static uint32_t
normfs_crc32c_hw_serial(uint32_t c, const uint8_t *data, size_t len)
{
	size_t i = 0u;

	while (len - i >= 8u) {
		uint64_t word;
		memcpy(&word, data + i, sizeof(word));
		c = NORMFS_CRC32C_STEP8(c, word);
		i += 8u;
	}

	while (i < len) {
		c = NORMFS_CRC32C_STEP1(c, data[i]);
		i++;
	}

	return c;
}

/*
 * One 3n-byte block as three independent n-byte streams: the crc
 * instruction's latency hides behind the other two chains, so the block runs
 * at instruction throughput instead. n must be a multiple of 8. Streams b and
 * c start from state 0, which makes their folds the constant terms of the
 * affine block maps, so the shift operators join the three results exactly.
 */
NORMFS_CRC32C_HW_TARGET static uint32_t
normfs_crc32c_hw_block3(uint32_t c0, const uint8_t *p, size_t n,
    const uint32_t m2n[32], const uint32_t mn[32])
{
	uint32_t a = c0;
	uint32_t b = 0u;
	uint32_t c = 0u;
	size_t i;

	for (i = 0u; i < n; i += 8u) {
		uint64_t wa;
		uint64_t wb;
		uint64_t wc;

		memcpy(&wa, p + i, sizeof(wa));
		memcpy(&wb, p + n + i, sizeof(wb));
		memcpy(&wc, p + 2u * n + i, sizeof(wc));
		a = NORMFS_CRC32C_STEP8(a, wa);
		b = NORMFS_CRC32C_STEP8(b, wb);
		c = NORMFS_CRC32C_STEP8(c, wc);
	}

	return normfs_crc32c_shift(a, m2n) ^ normfs_crc32c_shift(b, mn) ^ c;
}

/* The only place the 0xFFFFFFFF inversion convention lives on this path. */
NORMFS_CRC32C_HW_TARGET uint32_t
normfs_crc32c_hw(uint32_t crc, const uint8_t *data, size_t len)
{
	uint32_t c = ~crc;
	size_t i = 0u;

	while (len - i >= 3072u) {
		c = normfs_crc32c_hw_block3(c, data + i, 1024u,
		    normfs_crc32c_shift_2048, normfs_crc32c_shift_1024);
		i += 3072u;
	}

	while (len - i >= 768u) {
		c = normfs_crc32c_hw_block3(c, data + i, 256u,
		    normfs_crc32c_shift_512, normfs_crc32c_shift_256);
		i += 768u;
	}

	return ~normfs_crc32c_hw_serial(c, data + i, len - i);
}

/* Resolved once before main rather than per call: the feature query is not
 * free, and a plain load keeps the dispatcher's "assigns nothing" honest. */
int normfs_crc32c_hw_supported = 0;

__attribute__((constructor)) static void
normfs_crc32c_hw_detect(void)
{
#if defined(NORMFS_CRC32C_X86)
	normfs_crc32c_hw_supported = __builtin_cpu_supports("sse4.2") ? 1 : 0;
#elif defined(NORMFS_CRC32C_ARM_RUNTIME)
	normfs_crc32c_hw_supported =
	    (getauxval(AT_HWCAP) & HWCAP_CRC32) != 0ul ? 1 : 0;
#else
	normfs_crc32c_hw_supported = 1;
#endif
}

#endif

/*@ requires len == 0 || \valid_read(data + (0 .. len - 1));
    assigns \nothing;
    ensures \result == normfs_crc32c_logic(crc, data, len);
*/
uint32_t
normfs_crc32c(uint32_t crc, const uint8_t *data, size_t len)
{
#if defined(NORMFS_CRC32C_HW)
	if (normfs_crc32c_hw_supported != 0)
		return normfs_crc32c_hw(crc, data, len);
#endif
	return normfs_crc32c_portable(crc, data, len);
}
