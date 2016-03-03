on run {pwd, n} -- each arg is a string
    tell application "Terminal"
      set shell to do script "stack exec kvstore-client -- -l -n " & n
      do script "cd " & pwd in shell
  end tell
end run
