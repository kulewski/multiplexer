// IntrusiveValue<T>: a reference-counted T for boost::intrusive_ptr.
#ifndef MX_LIB_INTRUSIVE_VALUE_H_
#define MX_LIB_INTRUSIVE_VALUE_H_

namespace mx {

/*
 * forward decl.
 */
template <typename T> struct IntrusiveValue;
template <typename T> void intrusive_ptr_release(IntrusiveValue<T> *);
template <typename T> void intrusive_ptr_add_ref(IntrusiveValue<T> *);

// A value with an intrusive reference count, for sharing a plain T (a bool,
// say) through boost::intrusive_ptr without a separate control block.
template <typename T> struct IntrusiveValue {
  IntrusiveValue() : references_(0) {}
  IntrusiveValue(const T &value) : references_(0), value_(value) {}

  operator T &() { return value_; }
  operator const T &() const { return value_; }

  IntrusiveValue &operator=(const T &value) {
    value_ = value;
    return *this;
  }

  IntrusiveValue &operator=(const IntrusiveValue &other) {
    if (&other != this) {
      value_ = other.value_;
    }
    return *this;
  }

private:
  unsigned int references_;
  T value_;

  friend void intrusive_ptr_release<>(IntrusiveValue<T> *);
  friend void intrusive_ptr_add_ref<>(IntrusiveValue<T> *);
};

/*
 * boost::intrusive_ptr support
 */
template <typename T> void intrusive_ptr_release(IntrusiveValue<T> *itr) {
  if (--itr->references_ == 0)
    delete itr;
}

template <typename T> void intrusive_ptr_add_ref(IntrusiveValue<T> *itr) { ++itr->references_; }

}; // namespace mx

#endif // MX_LIB_INTRUSIVE_VALUE_H_
