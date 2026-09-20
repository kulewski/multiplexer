// mx::options::Options: every form the command line may take, the errors, and the help.
#include "lib/options.h"

#include <gtest/gtest.h>

using mx::options::Error;
using mx::options::Options;

TEST(Options, EveryValueForm) {
  std::string rules;
  std::string address;
  unsigned int every = 0;
  double timeout = 0;
  Options options;
  options.add("rules", &rules, "multiplexer.rules", "the rules");
  options.add("address,M", &address, "0.0.0.0:1980", "where to listen");
  options.add("every", &every, "N");
  options.add("timeout,t", &timeout, 5.0, "seconds");
  EXPECT_EQ("multiplexer.rules", rules);
  EXPECT_EQ(5.0, timeout);
  std::vector<std::string> args = {"--rules=other.rules", "-M", "127.0.0.1:0", "--every", "7", "-t2.5"};
  EXPECT_TRUE(options.parse(args).empty());
  EXPECT_EQ("other.rules", rules);
  EXPECT_EQ("127.0.0.1:0", address);
  EXPECT_EQ(7u, every);
  EXPECT_EQ(2.5, timeout);
  EXPECT_TRUE(options.given("rules"));
  EXPECT_FALSE(options.given("nothing"));
}

TEST(Options, SwitchesAndRepeats) {
  bool stay = true;
  std::vector<std::string> multiplexers;
  Options options;
  options.add_switch("stay", &stay, "keep running");
  options.add("multiplexer,M", &multiplexers, "an address, repeatable");
  EXPECT_FALSE(stay);
  std::vector<std::string> args = {"-M", "a:1", "--stay", "--multiplexer", "b:2", "--multiplexer=c:3"};
  options.parse(args);
  EXPECT_TRUE(stay);
  std::vector<std::string> expected = {"a:1", "b:2", "c:3"};
  EXPECT_EQ(expected, multiplexers);
  std::vector<std::string> none;
  options.parse(none);
  EXPECT_TRUE(stay) << "a switch keeps its value; only what given() reports resets";
  EXPECT_FALSE(options.given("stay"));
  EXPECT_FALSE(options.given("multiplexer"));
}

TEST(Options, Positional) {
  std::string action;
  std::vector<std::string> files;
  int type = 0;
  Options options;
  options.add("action", &action, "what to do").positional("action");
  options.add("file", &files, "inputs").positional("file", -1);
  options.add("type", &type, 0, "a filter");
  std::vector<std::string> args = {"start", "--type", "3", "one", "--", "--two", "-3"};
  EXPECT_TRUE(options.parse(args).empty());
  EXPECT_EQ("start", action);
  EXPECT_EQ(3, type);
  std::vector<std::string> expected = {"one", "--two", "-3"};
  EXPECT_EQ(expected, files);

  Options one;
  one.add("action", &action, "what").positional("action");
  std::vector<std::string> two = {"start", "stop"};
  EXPECT_THROW(one.parse(two), Error);
}

TEST(Options, Unrecognized) {
  bool help = false;
  std::string verbosity;
  Options general("General options");
  general.add_switch("help", &help, "help");
  general.add("verbosity", &verbosity, "MEDIUMVERBOSITY", "how much");
  std::vector<std::string> args = {"--verbosity", "CHATTERBOX", "run_multiplexer", "--rules", "x",
                                   "-M",          "a:1",        "--help"};
  std::vector<std::string> rest = general.parse(args, true);
  std::vector<std::string> expected = {"run_multiplexer", "--rules", "x", "-M", "a:1"};
  EXPECT_EQ(expected, rest);
  EXPECT_TRUE(help);
  EXPECT_EQ("CHATTERBOX", verbosity);
  EXPECT_THROW(general.parse(args), Error);
}

TEST(Options, Errors) {
  int count = 0;
  unsigned type = 0;
  bool flag = false;
  Options options;
  options.add("count", &count, 1, "N");
  options.add("type", &type, "the type").required();
  options.add_switch("flag", &flag, "a flag");
  std::vector<std::string> missing_required = {"--count", "2"};
  EXPECT_THROW(options.parse(missing_required), Error);
  std::vector<std::string> bad_number = {"--type", "1", "--count", "many"};
  EXPECT_THROW(options.parse(bad_number), Error);
  std::vector<std::string> missing_value = {"--type", "1", "--count"};
  EXPECT_THROW(options.parse(missing_value), Error);
  std::vector<std::string> switch_value = {"--type", "1", "--flag=yes"};
  EXPECT_THROW(options.parse(switch_value), Error);
  std::vector<std::string> twice = {"--type", "1", "--type", "2"};
  EXPECT_THROW(options.parse(twice), Error);
  std::vector<std::string> good = {"--type", "1", "--count=3", "--flag"};
  EXPECT_NO_THROW(options.parse(good));
  EXPECT_EQ(3, count);
  EXPECT_EQ(1u, type);
  EXPECT_TRUE(flag);
  try {
    options.parse(missing_required);
    FAIL();
  } catch (const Error& error) {
    EXPECT_EQ("the option '--type' is required but missing", std::string(error.what()));
  }
}

TEST(Options, Help) {
  std::string rules;
  bool tap = false;
  std::string hidden;
  Options options;
  options.add("rules,r", &rules, "multiplexer.rules", "the rules file");
  options.add_switch("allow-tap", &tap, "let peers tap");
  options.add("secret", &hidden, "not shown").hidden();
  std::ostringstream out;
  out << options;
  const std::string text = out.str();
  EXPECT_EQ(0u, text.find("Options:\n"));
  EXPECT_NE(std::string::npos, text.find("  -r, --rules ARG (=multiplexer.rules)  the rules file\n"));
  EXPECT_NE(std::string::npos, text.find("  --allow-tap"));
  EXPECT_NE(std::string::npos, text.find("let peers tap\n"));
  EXPECT_EQ(std::string::npos, text.find("secret"));
}
