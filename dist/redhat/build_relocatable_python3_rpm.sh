print_usage() {
    echo "$0 --reloc-pkg build/release/scylla-relocatable-python3.tar.gz"
    echo "  --reloc-pkg specify relocatable package path"
    echo "  --rpmbuild specify directory to use for building rpms"
    exit 1
}

RELOC_PKG=
RPMBUILD=
while [ $# -gt 0 ]; do
    case "$1" in
        "--reloc-pkg")
            RELOC_PKG=$2
            shift 2
            ;;
        "--rpmbuild")
            RPMBUILD=$2
            shift 2
            ;;
        *)
            print_usage
            ;;
    esac
done

if [ -z "$RELOC_PKG" ]; then
    print_usage
    exit 1
fi

if [ -z "$RPMBUILD" ]; then
    print_usage
    exit 1
fi

if [ ! -f "$RELOC_PKG" ]; then
    echo "$RELOC_PKG not found."
    exit 1
fi
RELOC_PKG_BASENAME=$(basename $RELOC_PKG)

RPMBUILD=$(readlink -f $RPMBUILD)
SPEC=$(dirname $(readlink -f $0))

mkdir -p $RPMBUILD/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS}
PYVER=$(python3 -V | cut -d' ' -f2)

ln -fv $RELOC_PKG $RPMBUILD/SOURCES/
pystache $SPEC/relocatable_python.spec.mustache "{ \"version\": \"$PYVER\", \"reloc_pkg\": \"$RELOC_PKG_BASENAME\", \"name\": \"scylla-relocatable-python3\", \"target\": \"/opt/scylladb/python3\" }" > $RPMBUILD/SPECS/relocatable_python.spec
rpmbuild --nodebuginfo -ba --define "_build_id_links none" --define "_topdir $RPMBUILD" --define "dist .el7" $RPMBUILD/SPECS/relocatable_python.spec
