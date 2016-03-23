// Copyright 2013 Google, Inc.
// All Rights Reserved.
//
// Utilities for container logging.

#ifndef UTIL_GTL_CONTAINER_LOGGING_H_
#define UTIL_GTL_CONTAINER_LOGGING_H_

#include <ostream>  // NOLINT

#include "supersonic/utils/integral_types.h"

namespace util {
namespace gtl {

// Several policy classes below determine how LogRangeToStream will
// format a range of items.  A Policy class should have these methods:
//
// Called to print an individual container element.
//   void Log(ostream &out, const ElementT &element) const;
//
// Called before printing the set of elements:
//   void LogOpening(ostream &out) const;
//
// Called after printing the set of elements:
//   void LogClosing(ostream &out) const;
//
// Called before printing the first element:
//   void LogFirstSeparator(ostream &out) const;
//
// Called before printing the remaining elements:
//   void LogSeparator(ostream &out) const;
//
// Returns the maximum number of elements to print:
//   int64 MaxElements() const;
//
// Called to print an indication that MaximumElements() was reached:
//   void LogEllipsis(ostream &out) const;

namespace internal {

struct LogBase {
  template <typename ElementT>
  void Log(std::ostream &out, const ElementT &element) const {  // NOLINT
    out << element;
  }
  void LogEllipsis(std::ostream &out) const {  // NOLINT
    out << "...";
  }
};

struct LogShortBase : public LogBase {
  void LogOpening(std::ostream &out) const { out << "["; }        // NOLINT
  void LogClosing(std::ostream &out) const { out << "]"; }        // NOLINT
  void LogFirstSeparator(std::ostream &out) const { out << ""; }  // NOLINT
  void LogSeparator(std::ostream &out) const { out << ", "; }     // NOLINT
};

struct LogMultilineBase : public LogBase {
  void LogOpening(std::ostream &out) const { out << "["; }          // NOLINT
  void LogClosing(std::ostream &out) const { out << "\n]"; }        // NOLINT
  void LogFirstSeparator(std::ostream &out) const { out << "\n"; }  // NOLINT
  void LogSeparator(std::ostream &out) const { out << "\n"; }       // NOLINT
};

struct LogLegacyBase : public LogBase {
  void LogOpening(std::ostream &out) const { out << ""; }         // NOLINT
  void LogClosing(std::ostream &out) const { out << ""; }         // NOLINT
  void LogFirstSeparator(std::ostream &out) const { out << ""; }  // NOLINT
  void LogSeparator(std::ostream &out) const { out << " "; }      // NOLINT
};

}  // namespace internal

// LogShort uses [] braces and separates items with comma-spaces.  For
// example "[1, 2, 3]".
struct LogShort : public internal::LogShortBase {
  int64 MaxElements() const { return kint64max; }
};

// LogShortUpTo100 formats the same as LogShort but prints no more
// than 100 elements.
struct LogShortUpTo100 : public internal::LogShortBase {
  int64 MaxElements() const { return 100; }
};

// LogMultiline uses [] braces and separates items with
// newlines.  For example "[
// 1
// 2
// 3
// ]".
struct LogMultiline : public internal::LogMultilineBase {
  int64 MaxElements() const { return kint64max; }
};

// LogMultilineUpTo100 formats the same as LogMultiline but
// prints no more than 100 elements.
struct LogMultilineUpTo100 : public internal::LogMultilineBase {
  int64 MaxElements() const { return 100; }
};

// The legacy behavior of LogSequence() does not use braces and
// separates items with spaces.  For example "1 2 3".
struct LogLegacyUpTo100 : public internal::LogLegacyBase {
  int64 MaxElements() const { return 100; }
};
struct LogLegacy : public internal::LogLegacyBase {
  int64 MaxElements() const { return kint64max; }
};

// The default policy for new code.
typedef LogShortUpTo100 LogDefault;

// LogRangeToStream should be used to define operator<< for
// STL and STL-like containers.  For example, see stl_logging.h.
template <typename IteratorT, typename PolicyT>
inline void LogRangeToStream(std::ostream &out,  // NOLINT
                             IteratorT begin, IteratorT end,
                             const PolicyT &policy) {
  policy.LogOpening(out);
  for (size_t i = 0; begin != end && i < policy.MaxElements(); ++i, ++begin) {
    if (i == 0) {
      policy.LogFirstSeparator(out);
    } else {
      policy.LogSeparator(out);
    }
    policy.Log(out, *begin);
  }
  if (begin != end) {
    policy.LogSeparator(out);
    policy.LogEllipsis(out);
  }
  policy.LogClosing(out);
}

namespace detail {

// RangeLogger is a helper class for util::gtl::LogRange and
// util::gtl::LogContainer; do not use it directly.  This object
// captures iterators into the argument of the LogRange and
// LogContainer functions, so its lifetime should be confined to a
// single logging statement.  Objects of this type should not be
// assigned to local variables.
template <typename IteratorT, typename PolicyT>
class RangeLogger {
 public:
  RangeLogger(const IteratorT &begin, const IteratorT &end,
                  const PolicyT &policy)
      : begin_(begin), end_(end), policy_(policy) { }

  friend std::ostream &operator<<(std::ostream &out, const RangeLogger &range) {
    util::gtl::LogRangeToStream<IteratorT, PolicyT>(
        out, range.begin_, range.end_, range.policy_);
    return out;
  }

 private:
  IteratorT begin_;
  IteratorT end_;
  PolicyT policy_;
};

}  // namespace detail

// Log a range using "policy".  For example:
//
//   LOG(INFO) << util::gtl::LogRange(start_pos, end_pos,
//                                    util::gtl::LogMultiline());
//
// The above example will print the range using newlines between
// elements, enclosed in [] braces.
template <typename IteratorT, typename PolicyT>
detail::RangeLogger<IteratorT, PolicyT> LogRange(
    const IteratorT &begin, const IteratorT &end, const PolicyT &policy) {
  return util::gtl::detail::RangeLogger<IteratorT, PolicyT>(
      begin, end, policy);
}

// Log a range.  For example:
//
//   LOG(INFO) << util::gtl::LogRange(start_pos, end_pos);
//
// By default, Range() uses the LogShortUpTo100 policy: comma-space
// separation, no newlines, and with limit of 100 items.
template <typename IteratorT>
detail::RangeLogger<IteratorT, LogDefault> LogRange(
    const IteratorT &begin, const IteratorT &end) {
  return util::gtl::LogRange(begin, end, LogDefault());
}

// Log a container using "policy".  For example:
//
//   LOG(INFO) << util::gtl::LogContainer(container,
//                                        util::gtl::LogMultiline());
//
// The above example will print the container using newlines between
// elements, enclosed in [] braces.
template <typename ContainerT, typename PolicyT>
detail::RangeLogger<typename ContainerT::const_iterator, PolicyT>
    LogContainer(const ContainerT &container, const PolicyT &policy) {
  return util::gtl::LogRange(container.begin(), container.end(), policy);
}

// Log a container.  For example:
//
//   LOG(INFO) << util::gtl::LogContainer(container);
//
// By default, Container() uses the LogShortUpTo100 policy: comma-space
// separation, no newlines, and with limit of 100 items.
template <typename ContainerT>
detail::RangeLogger<typename ContainerT::const_iterator, LogDefault>
    LogContainer(const ContainerT &container) {
  return util::gtl::LogContainer(container, LogDefault());
}

}  // namespace gtl
}  // namespace util

#endif  // UTIL_GTL_CONTAINER_LOGGING_H_
