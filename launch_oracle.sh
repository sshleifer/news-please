#SLURMLIST="inst-1peuj-elegant-pegasus,inst-4ilmp-elegant-pegasus,inst-4mhhw-elegant-pegasus,inst-6utfo-elegant-pegasus,inst-8cas1-elegant-pegasus,inst-bm1tl-elegant-pegasus,inst-c3vih-elegant-pegasus,inst-ceia2-elegant-pegasus,inst-cu41e-elegant-pegasus,inst-czlp2-elegant-pegasus,inst-dxwdy-elegant-pegasus,inst-e06o7-elegant-pegasus,inst-e7jdm-elegant-pegasus,inst-eg5rm-elegant-pegasus,inst-evudf-elegant-pegasus,inst-faunu-elegant-pegasus,inst-ieq5l-elegant-pegasus,inst-ixm8o-elegant-pegasus,inst-kwjdc-elegant-pegasus,inst-n8ztw-elegant-pegasus,inst-nmggj-elegant-pegasus,inst-pvwsj-elegant-pegasus,inst-qsk26-elegant-pegasus,inst-qukiw-elegant-pegasus,inst-rt9cr-elegant-pegasus,inst-szfph-elegant-pegasus,inst-t6h37-elegant-pegasus,inst-xl6if-elegant-pegasus,inst-z2ukx-elegant-pegasus"

#SLURMLIST="inst-0kygf-elegant-pegasus,inst-1peuj-elegant-pegasus"
PARTIAL_LIST="inst-cu41e-elegant-pegasus,inst-8cas1-elegant-pegasus,inst-c3vih-elegant-pegasus,inst-4mhhw-elegant-pegasus,inst-4ilmp-elegant-pegasus,inst-e7jdm-elegant-pegasus,inst-ieq5l-elegant-pegasus,inst-bm1tl-elegant-pegasus,inst-dxwdy-elegant-pegasus,inst-6utfo-elegant-pegasus,inst-n8ztw-elegant-pegasus,inst-evudf-elegant-pegasus,inst-e06o7-elegant-pegasus,inst-faunu-elegant-pegasus,inst-kwjdc-elegant-pegasus,inst-nmggj-elegant-pegasus,inst-szfph-elegant-pegasus,inst-czlp2-elegant-pegasus,inst-eg5rm-elegant-pegasus,inst-qukiw-elegant-pegasus,inst-xl6if-elegant-pegasus,inst-t6h37-elegant-pegasus,inst-ixm8o-elegant-pegasus,inst-pvwsj-elegant-pegasus,inst-z2ukx-elegant-pegasus,inst-qsk26-elegant-pegasus"
pdsh -w $PARTIAL_LIST /home/sam/news-please/launcher2.sh


ssh inst-xl6if-elegant-pegasus bash /home/sam/news-please/launcher2.sh

ssh inst-dxwdy-elegant-pegasus bash /home/sam/news-please/launcher2.sh &
ssh inst-c3vih-elegant-pegasus bash /home/sam/news-please/launcher2.sh &
sleep 10s && ssh inst-8cas1-elegant-pegasus bash /home/sam/news-please/launcher2.sh &
sleep 10s && ssh inst-4ilmp-elegant-pegasus bash /home/sam/news-please/launcher2.sh &
sleep 10s && ssh inst-6utfo-elegant-pegasus bash /home/sam/news-please/launcher2.sh &
sleep 10s && ssh inst-czlp2-elegant-pegasus bash /home/sam/news-please/launcher2.sh &
sleep 10s && ssh inst-nmggj-elegant-pegasus bash /home/sam/news-please/launcher2.sh &
sleep 10s && ssh inst-t6h37-elegant-pegasus bash /home/sam/news-please/launcher2.sh &
sleep 10s && ssh inst-szfph-elegant-pegasus bash /home/sam/news-please/launcher2.sh &
sleep 10s && ssh inst-faunu-elegant-pegasus bash /home/sam/news-please/launcher2.sh &
sleep 10s && ssh inst-cu41e-elegant-pegasus bash /home/sam/news-please/launcher2.sh &
sleep 10s && ssh inst-ixm8o-elegant-pegasus bash /home/sam/news-please/launcher2.sh &
sleep 10s && ssh inst-kwjdc-elegant-pegasus bash /home/sam/news-please/launcher2.sh &
sleep 10s && ssh inst-qsk26-elegant-pegasus bash /home/sam/news-please/launcher2.sh &
sleep 10s && ssh inst-4mhhw-elegant-pegasus bash /home/sam/news-please/launcher2.sh &
sleep 10s && ssh inst-qukiw-elegant-pegasus bash /home/sam/news-please/launcher2.sh &
sleep 10s && ssh inst-e7jdm-elegant-pegasus bash /home/sam/news-please/launcher2.sh &
sleep 10s && ssh inst-ieq5l-elegant-pegasus bash /home/sam/news-please/launcher2.sh &
sleep 10s && ssh inst-eg5rm-elegant-pegasus bash /home/sam/news-please/launcher2.sh &
sleep 10s && ssh inst-bm1tl-elegant-pegasus bash /home/sam/news-please/launcher2.sh &
sleep 10s && ssh inst-n8ztw-elegant-pegasus bash /home/sam/news-please/launcher2.sh &
sleep 10s && ssh inst-e06o7-elegant-pegasus bash /home/sam/news-please/launcher2.sh &
sleep 10s && ssh inst-evudf-elegant-pegasus bash /home/sam/news-please/launcher2.sh &
sleep 10s && ssh inst-pvwsj-elegant-pegasus bash /home/sam/news-please/launcher2.sh &
sleep 10s && ssh inst-z2ukx-elegant-pegasus bash /home/sam/news-please/launcher2.sh &


sleep 10s && ssh inst-0kygf-elegant-pegasus bash /home/sam/news-please/launcher2.sh &
sleep 10s && ssh inst-ceia2-elegant-pegasus bash /home/sam/news-please/launcher2.sh &