Summary: Python Ruote/AMQP client
Name: python3-ruote-amqp
Version: 2.4.0
Release: 1
Source0: %{name}-%{version}.tar.gz
License: UNKNOWN
Group: Development/Libraries
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot
Prefix: %{_prefix}
BuildRequires: python3
BuildRequires: python3-setuptools
BuildArch: noarch
Vendor: David Greaves <david@dgreaves.com>
Url: http://github.com/lbt/ruote-amqp-pyclient

%description
UNKNOWN

%prep
%setup -n %{name}-%{version}

%build
%python3_build

%install
%python3_install

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root)
%{python_sitelib}/RuoteAMQP
%{python_sitelib}/ruote_amqp*egg-info
