import io, os, sys, tempfile
sys.path.insert(0, os.path.abspath('src'))
os.chdir('src/weecfg/tests')
import configobj, weecfg, weecfg.update_config
_, config_dict = weecfg.read_config('weewx20_user.conf')
_, template = weecfg.read_config('../../weewx_data/weewx.conf')
weecfg.update_config.update_and_merge(config_dict, template)
fd = tempfile.NamedTemporaryFile(delete=False, suffix='.conf'); fd.close()
weecfg.save(config_dict, fd.name)
check = configobj.ConfigObj(fd.name, encoding='utf-8')
with io.BytesIO() as out:
    check.write(out); out.seek(0)
    io.open('expected/weewx43_user_expected.conf', 'wb').write(out.read())
os.unlink(fd.name)
print("written")
