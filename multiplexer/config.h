// The rules file in memory: peer types by number and by name, message types
// by number with their routing rules resolved to peer type numbers.
//
// Read from the protocol buffer text format described in docs/rules.md, at
// start and, by the multiplexer, again whenever the file changes (a fresh
// Config replaces the old one whole, see Server::load_rules). Config<std::map>
// is what the multiplexer and the client use;
// generate_constants.cc instantiates Config<std::multimap> so that it can
// report duplicate numbers instead of silently keeping one. A Config built
// without a file holds only the entries a client needs to recognize a
// multiplexer (BASIC_CONFIGURATION_DATA) and reports initialized() false,
// which relaxes the peer type check in ConnectionsManager.
#ifndef MX_MULTIPLEXER_CONFIG_H_
#define MX_MULTIPLEXER_CONFIG_H_

#include <google/protobuf/text_format.h>

#include <cstdint>
#include <fstream>
#include <iterator>
#include <map>
#include <set>
#include <string>
#include <type_traits>
#include <vector>

#include "lib/assertion.h"
#include "lib/exception.h"
#include "multiplexer/Multiplexer.pb.h" /* generated */

#ifndef IS_GENERATE_CONSTANTS
#include "lib/logging/logging.h"
#endif

namespace multiplexer {

namespace config_detail {
static const char* const BASIC_CONFIGURATION_DATA =
    "\
peer {\
    type: 1\
    name: \"MULTIPLEXER\"\
    comment: \"Peer type representing normal multiplexer instance.\"\
}\
type {\
    type: 2\
    name: \"CONNECTION_WELCOME\"\
    comment: \"message interchange by peers just after connecting to each other\"\
}\
";
};

// See the file comment. The map template parameter is std::map or
// std::multimap; everything else is the same.
template <template <typename, typename, typename, typename> class map_template_ = std::map>
class Config {
  /* static members */
 public:
  struct Exception : public mx::Exception {
    Exception(const std::string& explanation) : mx::Exception(explanation) {}
  };

 public:
  // The built-in minimum (a client's), or the rules read from `file`.
  Config() : initialized_(false), unknown_("UNKNOWN") { read_configuration(config_detail::BASIC_CONFIGURATION_DATA); }

  Config(const std::string& file) : initialized_(true), unknown_("UNKNOWN") { read_configuration(file); }

  // The rules in `text`, a rules file's contents read already, alone, as
  // a file gives them; `what` names them in the errors. Throws as
  // read_configuration_text does.
  static Config from_text(const std::string& text, const std::string& what) {
    Config config(Unread{});
    config.read_configuration_text(text, what);
    return config;
  }

  // Back to the built-in minimum.
  void clear() {
    *this = Config();
    Assert(!initialized());
  }

  // Replace the rules with those in `file` (protocol buffer text format).
  // Throws mx::Exception if the file is missing or does not parse, or
  // Config::Exception if it names an unknown peer.
  void read_configuration(const std::string& file) {
    using std::ifstream;

#ifndef IS_GENERATE_CONSTANTS
    using namespace mx::logging::consts;
    MX_LOG(DEBUG, HIGHVERBOSITY, CTX("config") TEXT("reading configuration file " + file));
#endif

    ifstream in(file.c_str(), ifstream::in | ifstream::binary);
    if (!in) {
      MXTHROW(mx::Exception("There is no config file '" + file + "'"));
    }
    const std::string text((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    read_configuration_text(text, file);
  }

  // As above from the file's contents already read; `what` names it in the
  // errors. The protobuf parser reports what is wrong with the text on
  // stderr, line and column included.
  void read_configuration_text(const std::string& text, const std::string& what) {
    MultiplexerRules rules;
    if (!google::protobuf::TextFormat::ParseFromString(text, &rules)) {
      MXTHROW(mx::Exception("cannot parse " + what + " as a rules file"));
    }
    if (rules.peer_size() == 0) {
      // Nothing could connect: a truncated file, or not a rules file at all.
      MXTHROW(mx::Exception("no peer types in " + what));
    }
    read_configuration(rules);
    initialized_ = true;
  }

  bool inline initialized() const { return initialized_; }

 private:
  // Nothing read: from_text's start, without the built-in minimum.
  struct Unread {};
  explicit Config(Unread) : initialized_(false), unknown_("UNKNOWN") {}

 public:
  // make 4-argument map_template_ 2-argument
  template <typename Key, typename Data>
  struct map_template {
    typedef map_template_<Key, Data, std::less<Key>, std::allocator<std::pair<const Key, Data>>> type;
  };

  typedef typename map_template<std::uint32_t, MultiplexerMessageDescription>::type MessageDescriptionById;
  // Whether this instantiation keeps one entry per number (std::map): then
  // a repeat in the file must be refused, since the other would be lost.
  static const bool REJECTS_DUPLICATES = std::is_same<typename map_template<int, int>::type, std::map<int, int>>::value;
  typedef typename map_template<std::string, MultiplexerPeerDescription>::type PeerDescriptionByName;
  typedef typename map_template<std::uint32_t, MultiplexerPeerDescription>::type PeerDescriptionById;

  // The three indexes: message types by number, peer types by name and by number.
  const MessageDescriptionById& message_description_by_id() const { return message_description_by_id_; }
  const PeerDescriptionByName& peer_by_name() const { return peer_by_name_; }
  const PeerDescriptionById& peer_by_type() const { return peer_by_type_; }

  /* shortcuts */
  // you must know this type is valid
  const std::string& message_name_by_type(std::uint32_t type) const {
    typename MessageDescriptionById::const_iterator entry = message_description_by_id_.find(type);
    Assert(!initialized() || entry != message_description_by_id_.end());
    return entry != message_description_by_id_.end() ? entry->second.name() : unknown_;
  }

  // you must know this type is valid
  const std::string& peer_name_by_type(std::uint32_t type) const {
    typename PeerDescriptionById::const_iterator entry = peer_by_type_.find(type);
    // The type may come from a peer's override_rrules, so it can be unknown.
    return entry != peer_by_type_.end() ? entry->second.name() : unknown_;
  }

  // Config's ownership
  const MultiplexerPeerDescription* peer_description(std::uint32_t type) const {
    return _valueptr_or_null(peer_by_type_, type);
  }

  // Config's ownership
  const MultiplexerMessageDescription* message_description(std::uint32_t type) const {
    return _valueptr_or_null(message_description_by_id_, type);
  }

 private:
  template <typename Key, typename Value, typename Compare, typename Alloc>
  const Value* _valueptr_or_null(const map_template_<Key, Value, Compare, Alloc>& description, const Key& key) const {
    typename map_template_<Key, Value, Compare, Alloc>::const_iterator entry = description.find(key);
    return entry == description.end() ? NULL : &entry->second;
  }

  /**
   * __insert(map, key, data)
   *	    inserts a copy of `data' into a map or multimap `map' at key `key'
   *	    returns a reference to the inserted copy
   */
  template <typename Key, typename Data, typename Compare, typename Alloc>
  Data& __insert(std::map<Key, Data, Compare, Alloc>& description, const Key& key, const Data& data) const {
    return description.insert(std::make_pair(key, data)).first->second;
  }

  template <typename Key, typename Data, typename Compare, typename Alloc>
  Data& __insert(std::multimap<Key, Data, Compare, Alloc>& description, const Key& key, const Data& data) const {
    return description.insert(std::make_pair(key, data))->second;
  }

  // Indexes the parsed rules. A routing rule names its peer type by name in
  // the file; here the number is filled in (set_peer_type), which is what
  // the server routes on. An unknown or missing name throws: better a
  // multiplexer that does not start, or keeps the rules it has, than one
  // that silently drops a type.
  void read_configuration(const MultiplexerRules& rules) {
    // validate the rules
    for (const MultiplexerMessageDescription& message_type : rules.type()) {
      for (const MultiplexerMessageDescription::RoutingRule& rule : message_type.to()) {
        if (!rule.has_peer()) {
          MXTHROW(typename Config::Exception("type " + message_type.name() + ": a `to` rule without a peer name"));
        }
      }
    }

    // A number or a name that repeats within the file is refused, as the
    // constants generator refuses it; the generator's own multimap
    // instantiation reports every duplicate itself and skips this.
    std::set<std::uint32_t> peer_types_seen;
    std::set<std::string> peer_names_seen;
    std::set<std::uint32_t> message_types_seen;
    std::set<std::string> message_names_seen;
    for (const MultiplexerPeerDescription& description : rules.peer()) {
      if (REJECTS_DUPLICATES && !peer_types_seen.insert(description.type()).second) {
        MXTHROW(typename Config::Exception("duplicate peer type " + std::to_string(description.type())));
      }
      if (REJECTS_DUPLICATES && !peer_names_seen.insert(description.name()).second) {
        MXTHROW(typename Config::Exception("duplicate peer name " + description.name()));
      }
      // A queue of 0 holds nothing: every message to the type would be
      // dropped, addressed ones too, while its peers stay connected.
      if (!description.queue_size()) {
        MXTHROW(typename Config::Exception("peer type " + std::to_string(description.type()) + " (" +
                                           description.name() + "): queue_size 0 holds no message"));
      }
      /* Multiplexer peer description */
      peer_by_type_.insert(std::make_pair(description.type(), description));
      peer_by_name_.insert(std::make_pair(description.name(), description));
    }

    for (const MultiplexerMessageDescription& message_type : rules.type()) {
      if (REJECTS_DUPLICATES && !message_types_seen.insert(message_type.type()).second) {
        MXTHROW(typename Config::Exception("duplicate message type " + std::to_string(message_type.type())));
      }
      if (REJECTS_DUPLICATES && !message_names_seen.insert(message_type.name()).second) {
        MXTHROW(typename Config::Exception("duplicate message name " + message_type.name()));
      }
      /* package descirption with routing rules definitions */
      MultiplexerMessageDescription& description =
          __insert(message_description_by_id_, message_type.type(), message_type);

      /* routing rules */
      for (MultiplexerMessageDescription::RoutingRule& rule : *description.mutable_to()) {
        std::map<std::string, MultiplexerPeerDescription>::iterator peer_entry = peer_by_name_.find(rule.peer());
        if (peer_entry == peer_by_name_.end()) {
          MXTHROW(typename Config::Exception("Unknown peer definition: '" + rule.peer() + "'"));
        }

        MultiplexerPeerDescription& peer = peer_entry->second;
        rule.set_peer_type(peer.type());
        // rule.clear_peer();
      }
    }
  }
  void read_configuration(const char* configuration_data) {
    MultiplexerRules rules;
    bool ok = google::protobuf::TextFormat::ParseFromString(configuration_data, &rules);
    AssertMsg(ok, "ParseFromIstream failed");
    read_configuration(rules);
  }

 private:
  bool initialized_;
  std::string unknown_;
  MessageDescriptionById message_description_by_id_;
  PeerDescriptionByName peer_by_name_;
  PeerDescriptionById peer_by_type_;
};

};  // namespace multiplexer

#endif  // MX_MULTIPLEXER_CONFIG_H_
