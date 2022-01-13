package com.farjad;

import com.farjad.meshjoin.LoaderWorker;
import com.farjad.meshjoin.ETWorker;

public class MeshJoin {

    public static void main(String[] args) {
        LoaderWorker loaderWorker = new LoaderWorker();
        loaderWorker.start();

        ETWorker etWorker = new ETWorker();
        etWorker.start();

        try {
            etWorker.join();
            loaderWorker.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
