%define name ruote-amqp-pyclient
%define version 0.2.1
%define unmangled_version 0.2.1
%define release 1

Summary: Python Ruote/AMQP client
Name: %{name}
Version: %{version}
Release: %{release}
Source0: %{name}_%{unmangled_version}.tar.gz
License: UNKNOWN
Group: Development/Libraries
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot
Prefix: %{_prefix}
BuildRequires: python,python-setuptools
BuildArch: noarch
Vendor: David Greaves <david@dgreaves.com>
Url: http://github.com/lbt/ruote-amqp-pyclient

%description
UNKNOWN

%prep
%setup -n %{name}-%{unmangled_version} -n %{name}-%{unmangled_version}

%build
python setup.py build

%install
python setup.py install --prefix=/usr --single-version-externally-managed --root=$RPM_BUILD_ROOT --record=INSTALLED_FILES

%clean
rm -rf $RPM_BUILD_ROOT

%files -f INSTALLED_FILES
%defattr(-,root,root)
/usr/lib/python2.6/site-packages/RuoteAMQP
