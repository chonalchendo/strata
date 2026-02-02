# install git hooks
[group('install')]
install-hooks:
    uvx prek install --hook-type=pre-push
    uvx prek install --hook-type=commit-msg
