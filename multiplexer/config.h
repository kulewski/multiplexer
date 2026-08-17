// The rules file in memory: peer types by number and by name, message types
// by number with their routing rules resolved to peer type numbers.
//
// Read once, at start, from the protocol buffer text format described in
// docs/rules.md. Config<std::map> is what the multiplexer and the client use;
// generate_constants.cc instantiates Config<std::multimap> so that it can
// report duplicate numbers instead of silently keeping one. A Config built
// without a file holds only the entries a client needs to recognize a
// multiplexer (BASIC_CONFIGURATION_DATA) and reports initialized() false,
// which relaxes the peer type check in ConnectionsManager.
#ifndef MX_MULTIPLEXER_CONFIG_H_
#define MX_MULTIPLEXER_CONFIG_H_

#include <boost/cstdint.hpp>
#include <boost/foreach.hpp>
#include <fstream>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/text_format.h>
#include <iostream>
#include <map>
#include <string>
#include <vector>

#include "lib/assertion.h"
#include "lib/exception.h"
#include "multiplexer/Multiplexer.pb.h" /* generated */

#ifndef IS_GENERATE_CONSTANTS
#include "lib/logging/logging.h"
#endif

namespace multiplexer {

namespace config_detail {
static const char *const BASIC_CONFIGURATION_DATA = "\
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
template <template <typename, typename, typename, typename> class map_template_ = std::map> class Config {
  /* static members */
public:
  struct Exception : public mx::Exception {
    Exception(const std::string &explanation) : mx::Exception(explanation) {}
  };

public:
  // The built-in minimum (a client's), or the rules read from `file`.
  Config() : initialized_(false), unknown_("UNKNOWN") { read_configuration(config_detail::BASIC_CONFIGURATION_DATA); }

  Config(const std::string &file) : initialized_(true), unknown_("UNKNOWN") { read_configuration(file); }

  // Back to the built-in minimum.
  void clear() {
    *this = Config();
    Assert(!initialized());
  }

  // Replace the rules with those in `file` (protocol buffer text format).
  // Throws mx::Exception if the file is missing or names an unknown peer.
  void read_configuration(const std::string &file) {
    using std::cout;
    using std::ifstream;

    MultiplexerRules rules;

#ifndef IS_GENERATE_CONSTANTS
    using namespace mx::logging::consts;
    MX_LOG(DEBUG, MEDIUMVERBOSITY, CTX("config") TEXT("reading configuration file"));
#endif

    ifstream in(file.c_str(), ifstream::in | ifstream::binary);
    if (!in) {
      MXTHROW(mx::Exception("There is no config file '" + file + "'"));
    }

    google::protobuf::io::IstreamInputStream zcis(&in);
    bool ok = google::protobuf::TextFormat::Parse(&zcis, &rules);
    AssertMsg(ok, "ParseFromIstream failed");
    Assert(in.eof());

    read_configuration(rules);
    initialized_ = true;
  }

  bool inline initialized() const { return initialized_; }

  // make 4-argument map_template_ 2-argument
  template <typename Key, typename Data> struct map_template {
    typedef map_template_<Key, Data, std::less<Key>, std::allocator<std::pair<const Key, Data>>> type;
  };

  typedef typename map_template<boost::uint32_t, MultiplexerMessageDescription>::type MessageDescriptionById;
  typedef typename map_template<std::string, MultiplexerPeerDescription>::type PeerDescriptionByName;
  typedef typename map_template<boost::uint32_t, MultiplexerPeerDescription>::type PeerDescriptionById;

  // The three indexes: message types by number, peer types by name and by number.
  const MessageDescriptionById &message_description_by_id() const { return message_description_by_id_; }
  const PeerDescriptionByName &peer_by_name() const { return peer_by_name_; }
  const PeerDescriptionById &peer_by_type() const { return peer_by_type_; }

  /* shortcuts */
  // you must know this type is valid
  const std::string &message_name_by_type(boost::uint32_t type) const {
    typename MessageDescriptionById::const_iterator entry = message_description_by_id_.find(type);
    Assert(!initialized() || entry != message_description_by_id_.end());
    return entry != message_description_by_id_.end() ? entry->second.name() : unknown_;
  }

  // you must know this type is valid
  const std::string &peer_name_by_type(boost::uint32_t type) const {
    typename PeerDescriptionById::const_iterator entry = peer_by_type_.find(type);
    // The type may come from a peer's override_rrules, so it can be unknown.
    return entry != peer_by_type_.end() ? entry->second.name() : unknown_;
  }

  // Config's ownership
  const MultiplexerPeerDescription *peer_description(boost::uint32_t type) const {
    return _valueptr_or_null(peer_by_type_, type);
  }

  // Config's ownership
  const MultiplexerMessageDescription *message_description(boost::uint32_t type) const {
    return _valueptr_or_null(message_description_by_id_, type);
  }

private:
  template <typename Key, typename Value, typename Compare, typename Alloc>
  const Value *_valueptr_or_null(const map_template_<Key, Value, Compare, Alloc> &description, const Key &key) const {
    typename map_template_<Key, Value, Compare, Alloc>::const_iterator entry = description.find(key);
    return entry == description.end() ? NULL : &entry->second;
  }

  /**
   * __insert(map, key, data)
   *	    inserts a copy of `data' into a map or multimap `map' at key `key'
   *	    returns a reference to the inserted copy
   */
  template <typename Key, typename Data, typename Compare, typename Alloc>
  Data &__insert(std::map<Key, Data, Compare, Alloc> &description, const Key &key, const Data &data) const {
    return description.insert(std::make_pair(key, data)).first->second;
  }

  template <typename Key, typename Data, typename Compare, typename Alloc>
  Data &__insert(std::multimap<Key, Data, Compare, Alloc> &description, const Key &key, const Data &data) const {
    return description.insert(std::make_pair(key, data))->second;
  }

  // Indexes the parsed rules. A routing rule names its peer type by name in
  // the file; here the number is filled in (set_peer_type), which is what
  // the server routes on. An unknown name is fatal: better a multiplexer
  // that does not start than one that silently drops a type.
  void read_configuration(const MultiplexerRules &rules) {
    // validate the rules
    BOOST_FOREACH (const MultiplexerMessageDescription &message_type, rules.type()) {
      BOOST_FOREACH (const MultiplexerMessageDescription::RoutingRule &rule, message_type.to()) {
        if (!rule.has_peer()) {
          std::cerr << "ERROR: MultiplexerMessageDescription::RoutingRule "
                       "without peer name\n";
          return;
        }
      }
    }

    BOOST_FOREACH (const MultiplexerPeerDescription &description, rules.peer()) {
      /* Multiplexer peer description */
      peer_by_type_.insert(std::make_pair(description.type(), description));
      peer_by_name_.insert(std::make_pair(description.name(), description));
    }

    BOOST_FOREACH (const MultiplexerMessageDescription &message_type, rules.type()) {
      /* package descirption with routing rules definitions */
      MultiplexerMessageDescription &description =
          __insert(message_description_by_id_, message_type.type(), message_type);

      /* routing rules */
      BOOST_FOREACH (MultiplexerMessageDescription::RoutingRule &rule, *description.mutable_to()) {
        std::map<std::string, MultiplexerPeerDescription>::iterator peer_entry = peer_by_name_.find(rule.peer());
        if (peer_entry == peer_by_name_.end())
          MXTHROW(typename Config::Exception("Unknown peer definition: '" + rule.peer() + "'"));

        MultiplexerPeerDescription &peer = peer_entry->second;
        rule.set_peer_type(peer.type());
        // rule.clear_peer();
      }
    }
  }
  void read_configuration(const char *configuration_data) {
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

}; // namespace multiplexer

#endif // MX_MULTIPLEXER_CONFIG_H_
