if [ "$TOX_ENV" = "py34" ]
then
   pip install coveralls
   coveralls
fi
