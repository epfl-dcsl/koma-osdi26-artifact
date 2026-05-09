#include <linux/bpf.h>
/* Only use this for debug output. Notice output from bpf_trace_printk()
 * end-up in /sys/kernel/debug/tracing/trace_pipe
 */

#define CMD_GET 0x00
#define CMD_GETK 0x0c
#define CMD_SET 0x01

// SEC("memcached_koma")
ssize_t memcached_koma(struct __sk_buff *skb) {
  /*return 4;*/
  struct __attribute__((__packed__)) binary_header_t {
    __u8 magic;
    __u8 opcode;
    __u32 id;
    __u16 key_len;
    __u8 extra_len;
    __u8 data_type;
    union {
      __u16 vbucket; // request use
      __u16 status;  // response use
    };
    __u32 body_len;
    __u32 opaque;
    __u64 version;
  };

  struct binary_header_t *memcached_header;
  struct _strp_msg *stm;
  __u32 header_len;
  __u8 opcode, extra_len;
  __u16 key_len;
  __u64 body_len;
  __u32 total_len;
  int skb_offset;

  header_len = sizeof(struct binary_header_t);
  key_len = memcached_header->key_len;
  extra_len = memcached_header->extra_len;
  body_len = memcached_header->body_len;
  opcode = memcached_header->opcode;
  skb_offset = *((int *)skb->cb);

  if (bpf_skb_load_bytes(skb,
                         offsetof(struct binary_header_t, key_len) + skb_offset,
                         &key_len, sizeof(key_len))) {
    return 0;
  }

  if (bpf_skb_load_bytes(
          skb, offsetof(struct binary_header_t, extra_len) + skb_offset,
          &extra_len, sizeof(extra_len))) {
    return 0;
  }

  if (bpf_skb_load_bytes(
          skb, offsetof(struct binary_header_t, body_len) + skb_offset,
          &body_len, sizeof(body_len))) {
    return 0;
  }

  if (bpf_skb_load_bytes(skb,
                         offsetof(struct binary_header_t, opcode) + skb_offset,
                         &opcode, sizeof(opcode))) {
    return 0;
  }

  if (opcode == CMD_SET) {
    return header_len + bpf_ntohs(body_len);
  } else {
    // bpf_trace_printk("The length of key is %d\n", bpf_ntohs(key_len));
    return header_len + bpf_ntohs(key_len) + bpf_ntohs(extra_len);
  }
}

// char _license[] SEC("license") = "GPL";
